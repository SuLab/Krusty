from datetime import datetime
from itertools import chain
import configargparse

import pandas as pd
from tqdm import tqdm
from wikidataintegrator import wdi_core, wdi_helpers, wdi_login
import textwrap


class Bot:
    equiv_prop_pid = None  # http://www.w3.org/2002/07/owl#equivalentProperty

    def __init__(self, node_path, edge_path, mediawiki_api_url, sparql_endpoint_url,
                 login, simulate=False):
        self.node_path = node_path
        self.edge_path = edge_path
        self.nodes = None
        self.edges = None
        self.parse_nodes_edges()
        self.login = login
        self.write = not simulate
        self.mediawiki_api_url = mediawiki_api_url
        self.sparql_endpoint_url = sparql_endpoint_url
        self.dbxref_pid = None

        self.item_engine = wdi_core.WDItemEngine.wikibase_item_engine_factory(mediawiki_api_url=mediawiki_api_url,
                                                                              sparql_endpoint_url=sparql_endpoint_url)
        self.get_equiv_prop_pid()
        self.uri_pid = wdi_helpers.id_mapper(self.get_equiv_prop_pid(), endpoint=sparql_endpoint_url)

        ####
        # these lines have to be done in this order because we need dbxref first before the others can be created
        now = datetime.utcnow().replace(microsecond=0)
        dbxref_pid, created = self.create_dbxref_prop()
        self.dbxref_pid = self.uri_pid['http://www.geneontology.org/formats/oboInOwl#DbXref']
        self.create_initial_props()
        # this fails on first run because the sparql endpoint has not yet been updated. so we need to wait
        if created:
            wdi_helpers.wait_for_last_modified(now, entity="http://wikibase.svc", delay=20,
                                               endpoint=sparql_endpoint_url)
        self.dbxref_qid = wdi_helpers.id_mapper(self.dbxref_pid, endpoint=sparql_endpoint_url)

        ####

    def create_dbxref_prop(self):
        # dbxref is special because other props use it
        dbxref_pid, created = self.create_property("External ID",
                                                   "generic property for holding a (generally CURIE-fied) external ID",
                                                   "string", "http://www.geneontology.org/formats/oboInOwl#DbXref",
                                                   "oboInOwl:DbXref")
        return dbxref_pid, created

    def create_initial_props(self):
        # properties we need regardless of whats in the edges file
        self.create_property("exact match", "v1", "string", "http://www.w3.org/2004/02/skos/core#exactMatch",
                             "skos:exactMatch")
        self.create_property("exact match", "v2", "string", "http://www.w3.org/2004/02/skos/core#skos_exactMatch",
                             "skos:exactMatch")
        self.create_property("reference uri", "", "url", "http://www.wikidata.org/entity/P854", "reference_uri")
        self.create_property("reference supporting text", "", "string", "http://reference_supporting_text",
                             "ref_supp_text")
        # this is to be used for the neo4j type, which it calls ":LABEL"
        self.create_property("type", "the neo4j type, aka ':LABEL'", "wikibase-item", "http://type", "type")

    def run(self, force=False):
        self.create_properties()
        self.create_classes()
        self.create_nodes(force=force)
        self.create_edges()

    def create_properties(self):
        # Reads the neo4j edges file to determine properties it needs to create
        edges = self.edges
        # make sure this edges we need exist
        curie_label = dict(zip(edges[':TYPE'], edges['property_label']))
        curie_label = {k: v for k, v in curie_label.items() if k}
        curie_label = {k: v if v else k for k, v in curie_label.items()}
        curie_uri = dict(zip(edges[':TYPE'], edges['property_uri']))

        # hard coding these because they're missing or wrong in the edges file
        curie_uri['colocalizes_with'] = "http://purl.obolibrary.org/obo/RO_0002325"
        curie_uri['contributes_to'] = "http://purl.obolibrary.org/obo/RO_0002326"
        # curie_uri['skos:exactMatch'] = "http://www.w3.org/2004/02/skos/core#exactMatch"

        # all edges will be an item except for skos:exactMatch
        if 'skos:exactMatch' in curie_label:
            del curie_label['skos:exactMatch']
        for curie, label in curie_label.items():
            self.create_property(label, "", "wikibase-item", curie_uri[curie], curie)

    def create_property(self, label, description, property_datatype, uri, dbxref):
        # returns tuple (property PID: str, created: bool)
        if uri in self.uri_pid:
            print("property already exists: {} {}".format(self.uri_pid[uri], uri))
            return (self.uri_pid[uri], False)
        s = [wdi_core.WDUrl(uri, self.get_equiv_prop_pid())]
        if self.dbxref_pid:
            s.append(wdi_core.WDString(dbxref, self.dbxref_pid))
        item = self.item_engine(item_name=label, domain="foo", data=s, core_props=[self.equiv_prop_pid])
        item.set_label(label)
        item.set_description(description)
        if self.write:
            item.write(self.login, entity_type="property", property_datatype=property_datatype)
        self.uri_pid[uri] = item.wd_item_id
        return (self.uri_pid[uri], True)

    def create_item(self, label, description, ext_id, synonyms=None, type_of=None, force=False):
        if (not force) and ext_id in self.dbxref_qid:
            print("item already exists: {} {}".format(self.dbxref_qid[ext_id], ext_id))
            return None
        s = [wdi_core.WDString(ext_id, self.dbxref_pid)]
        if type_of:
            s.append(wdi_core.WDItemID(self.dbxref_qid[type_of], self.uri_pid['http://type']))

        item = self.item_engine(item_name=label, domain="foo", data=s, core_props=[self.dbxref_pid])
        item.set_label(label)
        if description:
            item.set_description(description)
        if synonyms:
            item.set_aliases(synonyms)
        if self.write:
            item.write(self.login)
        self.dbxref_qid[ext_id] = item.wd_item_id

    def get_equiv_prop_pid(self):
        if self.equiv_prop_pid:
            return self.equiv_prop_pid
        # get the equivalent property property without knowing the PID for equivalent property!!!
        query = '''SELECT * WHERE {
          ?item ?prop <http://www.w3.org/2002/07/owl#equivalentProperty> .
          ?item <http://wikiba.se/ontology#directClaim> ?prop .
        }'''
        pid = wdi_core.WDItemEngine.execute_sparql_query(query, endpoint=self.sparql_endpoint_url)
        pid = pid['results']['bindings'][0]['prop']['value']
        pid = pid.split("/")[-1]
        self.equiv_prop_pid = pid
        return self.equiv_prop_pid

    def create_classes(self):
        # from the nodes file, get the "type", which neo4j calls ":LABEL" for some strange reason
        types = set(self.nodes[':LABEL'])
        for t in types:
            self.create_item(t, "", t)

    def create_nodes(self, force=False):
        nodes = self.nodes
        curie_label = dict(zip(nodes['id:ID'], nodes['preflabel']))
        curie_label = {k: v for k, v in curie_label.items() if k}
        curie_label = {k: v if v else k for k, v in curie_label.items()}
        curie_synonyms = dict(zip(nodes['id:ID'], nodes['synonyms:IGNORE'].map(lambda x: x.split("|") if x else [])))
        curie_descr = dict(zip(nodes['id:ID'], nodes['description']))
        curie_name = dict(zip(nodes['id:ID'], nodes['name']))
        curie_type = dict(zip(nodes['id:ID'], nodes[':LABEL']))

        curie_label = sorted(curie_label.items(), key=lambda x: x[0])
        t = tqdm(curie_label)
        for curie, label in t:
            t.set_description(label)
            if len(curie) > 100:
                continue
            synonyms = (set(curie_synonyms[curie]) | {curie_name[curie]}) - {label} - {''}
            self.create_item(label, curie_descr[curie], curie, synonyms=synonyms,
                             type_of=curie_type[curie], force=force)

    def create_edges(self):
        edges = self.edges

        subj_edges = edges.groupby(":START_ID")

        # subj, rows = "UniProt:Q96IV0", edges[edges[':START_ID']=='ClinVarVariant:50962']
        for subj, rows in tqdm(subj_edges, total=len(subj_edges)):
            subj = self.dbxref_qid.get(rows.iloc[0][':START_ID'])
            ss = self.create_subj_edges(rows)
            if not ss:
                continue
            item = self.item_engine(wd_item_id=subj, data=ss, domain="asdf")
            wdi_helpers.try_write(item, rows.iloc[0][':START_ID'], self.dbxref_pid, self.login, write=self.write)

    def create_subj_edges(self, rows):
        # input is a dataframe where all the subjects are the same
        # i.e. write to one item
        spo_edges = rows.groupby([":START_ID", ":TYPE", ":END_ID"])
        # spo, spo_rows = ('UniProt:Q96IV0', 'RO:0002331', 'GO:0006517'), rows[rows[':END_ID'] == 'GO:0006517']
        ss = []
        for spo, spo_rows in spo_edges:
            refs = self.create_statement_ref(spo_rows)
            s = self.create_statement(spo_rows.iloc[0])
            if not s:
                continue
            s.references = refs
            ss.append(s)
        return ss

    # noinspection PyTypeChecker
    def create_statement_ref(self, rows):
        """
        Ref supporting text gets split up into chunks of 400 chars each.
        if the ref url is from pubmed, it gets split. Otherwise it gets cropped to 400 chars
        """

        ref_url_pid = self.uri_pid['http://www.wikidata.org/entity/P854']
        ref_supp_text_pid = self.uri_pid['http://reference_supporting_text']
        refs = []
        for _, row in rows.iterrows():
            # textwrap.wrap splits lines on spaces only
            lines = textwrap.wrap(row.reference_supporting_text, 400, break_long_words=False)
            ref = [wdi_core.WDString(rst_chunk, ref_supp_text_pid, is_reference=True) for rst_chunk in lines]
            if row.reference_uri:
                for ref_uri in row.reference_uri.split("|"):
                    ref_uri = self.handle_special_ref_url(ref_uri)
                    if ref_uri.startswith("https://www.ncbi.nlm.nih.gov/pubmed/"):
                        ref.extend([wdi_core.WDUrl(this_url, ref_url_pid, is_reference=True)
                                    for this_url in self.split_pubmed_url(ref_uri)])
                    else:
                        ref.append(wdi_core.WDUrl(ref_uri[:400], ref_url_pid, is_reference=True))
            refs.append(ref)
        return refs

    def create_statement(self, row):
        subj = self.dbxref_qid.get(row[':START_ID'])
        pred = self.uri_pid.get(row['property_uri'])
        if row[':TYPE'] == "skos:exactMatch":
            obj = row[':END_ID']
        else:
            obj = self.dbxref_qid.get(row[':END_ID'])

        # print(subj, pred, obj)
        if not (subj and pred and obj):
            return None
        if row[':TYPE'] == "skos:exactMatch":
            s = wdi_core.WDString(obj, pred)
        else:
            s = wdi_core.WDItemID(obj, pred)
        return s

    @staticmethod
    def handle_special_ref_url(url):
        if url.startswith("ISBN"):
            isbn = url.replace("ISBN-13:", "").replace("ISBN-10", "")
            url = "https://www.wikidata.org/wiki/Special:BookSources/{}".format(isbn)
        return url

    @staticmethod
    def split_pubmed_url(url):
        base_url = "https://www.ncbi.nlm.nih.gov/pubmed/"
        url = url.replace(base_url, "")
        pmids = url.split(",")

        urls = []
        while pmids:
            this_url = base_url
            this_url += pmids.pop(0)
            while pmids and (len(this_url) + len(pmids[0]) + 1 < 400):
                this_url += ","
                this_url += pmids.pop(0)
            urls.append(this_url)

        return urls

    @staticmethod
    def join_pubmed_url(urls):
        base_url = "https://www.ncbi.nlm.nih.gov/pubmed/"
        urls = [x.replace(base_url, "") for x in urls]
        pmids = list(chain(*[x.split(",") for x in urls]))

        url = base_url + ",".join(pmids)

        return url

    def parse_nodes_edges(self):
        node_path, edge_path = self.node_path, self.edge_path
        edges = pd.read_csv(edge_path, dtype=str)
        edges = edges.fillna("")
        edges = edges.replace('None', "")
        nodes = pd.read_csv(node_path, dtype=str)
        nodes = nodes.fillna("")
        nodes = nodes.replace('None', "")

        """
        edges = edges[edges[':TYPE'] == "rdf:type"]
        s = set(edges[':START_ID'])
        nodes = nodes[nodes['id:ID'].isin(s)]
        """

        # handle nodes with no label
        blank = (nodes.preflabel == "") & (nodes.name == "")
        nodes.loc[blank, "preflabel"] = nodes.loc[blank, "id:ID"]

        # handle non-unique labels
        dupe = nodes.duplicated(subset=['preflabel'], keep=False)
        # append the ID to the label
        nodes.loc[dupe, "preflabel"] = nodes.loc[dupe, "preflabel"] + " (" + nodes.loc[dupe, "id:ID"] + ")"

        self.nodes, self.edges = nodes, edges


def main(user, password, mediawiki_api_url, sparql_endpoint_url, node_path, edge_path, simulate=False):
    login = wdi_login.WDLogin(user=user, pwd=password, mediawiki_api_url=mediawiki_api_url)
    bot = Bot(node_path, edge_path, mediawiki_api_url, sparql_endpoint_url, login, simulate=simulate)
    bot.run(force=False)


if __name__ == '__main__':
    p = configargparse.ArgParser(default_config_files=['config.cfg'])
    p.add('-c', '--config', is_config_file=True, help='config file path')
    p.add("--user", required=True, help="Wikibase username")
    p.add("--password", required=True, help="Wikibase password")
    p.add("--mediawiki_api_url", required=True, help="Wikibase mediawiki api url")
    p.add("--sparql_endpoint_url", required=True, help="Wikibase sparql endpoint url")
    p.add("--node-path", required=True, help="path to neo4j nodes csv dump")
    p.add("--edge-path", required=True, help="path to neo4j edges csv dump")
    p.add("--simulate", action='store_true', help="don't actually perform writes to Wikibase")
    options, _ = p.parse_known_args()
    d = options.__dict__.copy()
    del d['config']
    main(**d)
