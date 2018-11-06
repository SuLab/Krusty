import os
from itertools import chain

import pandas as pd
from tqdm import tqdm
from wikidataintegrator import wdi_core, wdi_helpers, wdi_login
from more_itertools import chunked

pd.options.display.width = 200
pd.set_option("display.max_column", 12)

try:
    from local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

HOST = "100.25.145.12"
WIKIBASE_PORT = "8181"
WDQS_PORT = "8282"
mediawiki_api_url = "http://{}:{}/w/api.php".format(HOST, WIKIBASE_PORT)
sparql_endpoint_url = "http://{}:{}/proxy/wdqs/bigdata/namespace/wdq/sparql".format(HOST, WDQS_PORT)


class Bot:
    equiv_prop_pid = None  # http://www.w3.org/2002/07/owl#equivalentProperty
    equiv_class_pid = None  # http://www.w3.org/2002/07/owl#equivalentClass
    dbxref_pid = None  # http://www.geneontology.org/formats/oboInOwl#DbXref

    def __init__(self, nodes, edges, login, write=True, run_one=False):
        self.nodes = nodes
        self.edges = edges
        self.login = login
        self.write = write
        self.run_one = run_one

        self.item_engine = wdi_core.WDItemEngine.wikibase_item_engine_factory(mediawiki_api_url=mediawiki_api_url,
                                                                              sparql_endpoint_url=sparql_endpoint_url)

        self.uri_pid = wdi_helpers.id_mapper(self.get_equiv_prop_pid(), endpoint=sparql_endpoint_url)
        self.dbxref_pid = self.uri_pid['http://www.geneontology.org/formats/oboInOwl#DbXref']
        self.dbxref_qid = wdi_helpers.id_mapper(self.dbxref_pid, endpoint=sparql_endpoint_url)

    def run(self):
        self.create_properties()
        self.create_classes()

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
        del curie_label['skos:exactMatch']
        for curie, label in curie_label.items():
            self.create_property(label, "", "wikibase-item", curie_uri[curie])

        self.create_property("exact match", "", "string", curie_uri["skos:exactMatch"])
        self.create_property("stated in", "", "wikibase-item", "http://www.wikidata.org/entity/P248")
        self.create_property("reference uri", "", "url", "http://www.wikidata.org/entity/P854")
        self.create_property("reference supporting text", "", "string", "http://reference_supporting_text")

    def create_property(self, label, description, property_datatype, uri):
        if uri in self.uri_pid:
            print("property already exists: {} {}".format(self.uri_pid[uri], uri))
            return None
        s = [wdi_core.WDUrl(uri, self.get_equiv_prop_pid())]
        item = self.item_engine(item_name=label, domain="foo", data=s, core_props=[self.dbxref_pid])
        item.set_label(label)
        item.set_description(description)
        item.write(self.login, entity_type="property", property_datatype=property_datatype)
        self.uri_pid[uri] = item.wd_item_id

    def create_item(self, label, description, ext_id, synonyms=None, type_of=None, force=False):
        if (not force) and ext_id in self.dbxref_qid:
            print("item already exists: {} {}".format(self.dbxref_qid[ext_id], ext_id))
            return None
        s = [wdi_core.WDString(ext_id, self.dbxref_pid)]
        if type_of:
            s.append(wdi_core.WDItemID(self.dbxref_qid[type_of],
                                       self.uri_pid['http://www.w3.org/1999/02/22-rdf-syntax-ns#rdf_type']))

        item = self.item_engine(item_name=label, domain="foo", data=s, core_props=[self.dbxref_pid])
        item.set_label(label)
        if description:
            item.set_description(description)
        if synonyms:
            item.set_aliases(synonyms)
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
        pid = wdi_core.WDItemEngine.execute_sparql_query(query, endpoint=sparql_endpoint_url)
        pid = pid['results']['bindings'][0]['prop']['value']
        pid = pid.split("/")[-1]
        self.equiv_prop_pid = pid
        return pid

    def get_equiv_class_pid(self):
        if self.equiv_class_pid:
            return self.equiv_class_pid
        d = wdi_helpers.id_mapper(self.get_equiv_prop_pid())
        self.equiv_class_pid = d['http://www.w3.org/2002/07/owl#equivalentClass']
        return self.equiv_class_pid

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
            wdi_helpers.try_write(item, rows.iloc[0][':START_ID'], self.dbxref_pid, self.login)

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
            rst = chunked(row.reference_supporting_text, 400)
            ref = [
                wdi_core.WDString("".join(rst_chunk).strip(), ref_supp_text_pid, is_reference=True)
                for rst_chunk in rst]
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



if __name__ == '__main__':
    edges = pd.read_csv("ngly1_statements.csv.gz", dtype=str)
    edges = edges.fillna("")
    edges = edges.replace('None', "")
    nodes = pd.read_csv("ngly1_concepts.csv.gz", dtype=str)
    nodes = nodes.fillna("")
    nodes = nodes.replace('None', "")

    # handle nodes with no label
    blank = (nodes.preflabel == "") & (nodes.name == "")
    nodes.loc[blank, "preflabel"] = nodes.loc[blank, "id:ID"]

    # handle non-unique label/descr
    dupe = nodes.duplicated(subset=['preflabel'], keep=False)
    # append the ID to the label
    nodes.loc[dupe, "preflabel"] = nodes.loc[dupe, "preflabel"] + " (" + nodes.loc[dupe, "id:ID"] + ")"

    login = wdi_login.WDLogin(user=WDUSER, pwd=WDPASS, mediawiki_api_url=mediawiki_api_url)
    bot = Bot(nodes, edges, login)

    bot.create_properties()
    bot.create_classes()
    bot.create_nodes()
    bot.create_edges()
