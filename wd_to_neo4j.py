import configargparse
import pandas as pd
from tqdm import tqdm
from wikidataintegrator import wdi_core, wdi_helpers
from more_itertools import chunked


class Bot:
    edge_columns = [':START_ID', ':TYPE', ':END_ID', 'reference_uri', 'reference_supporting_text',
                    'reference_date', 'property_label', 'property_description:IGNORE', 'property_uri']
    node_columns = ['id:ID', ':LABEL', 'preflabel', 'synonyms:IGNORE', 'name', 'description']

    def __init__(self, sparql_endpoint_url, mediawiki_api_url, node_out_path, edge_out_path):
        self.sparql_endpoint_url = sparql_endpoint_url
        self.mediawiki_api_url = mediawiki_api_url
        self.node_out_path = node_out_path
        self.edge_out_path = edge_out_path

        uri_pid = wdi_helpers.id_mapper("P2", endpoint=sparql_endpoint_url)
        self.pid_uri = {v: k for k, v in uri_pid.items()}
        dbxref_pid = uri_pid['http://www.geneontology.org/formats/oboInOwl#DbXref']
        dbxref_qid = wdi_helpers.id_mapper(dbxref_pid, endpoint=sparql_endpoint_url)
        self.qid_dbxref = {v: k for k, v in dbxref_qid.items()}
        self.ref_supp_text_pid = uri_pid["http://reference_supporting_text"]
        self.reference_uri_pid = uri_pid["http://www.wikidata.org/entity/P854"]
        self.instance_of_pid = uri_pid["http://www.w3.org/1999/02/22-rdf-syntax-ns#rdf_type"]

        # prop label and descriptions
        pids = {x for x in self.qid_dbxref if x.startswith("P")}
        props = wdi_core.WDItemEngine.generate_item_instances(list(pids), mediawiki_api_url)
        self.pid_label = {pid: item.get_label() for pid, item in props}
        self.pid_descr = {pid: item.get_description() for pid, item in props}

        # get all items and all statements
        qids = {x for x in self.qid_dbxref if x.startswith("Q")}
        self.item_iter = self.item_chunker(sorted(list(qids)))

        self.edge_lines = []
        self.node_lines = []

    def item_chunker(self, qids) -> wdi_core.WDItemEngine:
        # iterate through item instances, getting 20 at a time
        chunks = chunked(qids, 20)
        for chunk in chunks:
            items = wdi_core.WDItemEngine.generate_item_instances(chunk, mediawiki_api_url=self.mediawiki_api_url)
            for item in items:
                yield item[1]

    def parse_node(self, item: wdi_core.WDItemEngine):
        type_statements = [s for s in item.statements if s.get_prop_nr() == self.instance_of_pid]
        if len(type_statements) != 1:
            return None
        node_template = dict()
        node_template[':LABEL'] = self.qid_dbxref["Q" + str(type_statements[0].get_value())]
        node_template['id:ID'] = self.qid_dbxref[item.wd_item_id]
        node_template['preflabel'] = item.get_label()
        node_template['name'] = item.get_label()
        node_template['description'] = item.get_description()
        node_template['synonyms:IGNORE'] = "|".join(item.get_aliases())

        return node_template

    def write_out(self):
        df_edges = pd.DataFrame(self.edge_lines)
        df_edges['reference_date'] = None
        df_edges = df_edges[self.edge_columns]
        df_edges.to_csv(self.edge_out_path, index=None)

        df_nodes = pd.DataFrame(self.node_lines)
        df_nodes = df_nodes[self.node_columns]
        df_nodes.to_csv(self.node_out_path, index=None)

    def handle_statement(self, s, start_id):
        # if a statement has multiple refs, it will return multiple lines
        skip_statements = {"http://www.w3.org/1999/02/22-rdf-syntax-ns#rdf_type",
                           "http://www.geneontology.org/formats/oboInOwl#DbXref"}
        edge_lines = []
        line = {":START_ID": start_id, 'property_uri': self.pid_uri[s.get_prop_nr()]}
        if line['property_uri'] in skip_statements:
            return edge_lines
        line['property_label'] = self.pid_label[s.get_prop_nr()]
        line['property_description:IGNORE'] = self.pid_descr[s.get_prop_nr()]
        line[':TYPE'] = self.qid_dbxref[s.get_prop_nr()]
        line[':END_ID'] = self.qid_dbxref["Q" + str(s.get_value())] if s.data_type == "wikibase-item" else s.get_value()
        if s.references:
            for ref in s.references:
                ref_supp_text_statements = [x for x in ref if x.get_prop_nr() == self.ref_supp_text_pid]
                ref_supp_text = " ".join([x.get_value() for x in ref_supp_text_statements])
                reference_uri_statements = [x for x in ref if x.get_prop_nr() == self.reference_uri_pid]
                reference_uri = "|".join([x.get_value() for x in reference_uri_statements])
                # todo: rejoin split pubmed urls
                line['reference_supporting_text'] = ref_supp_text
                line['reference_uri'] = reference_uri
                edge_lines.append(line.copy())
        else:
            edge_lines.append(line.copy())
        return edge_lines

    def run(self):

        edge_lines = []
        node_lines = []
        for item in tqdm(self.item_iter):
            sub_qid = item.wd_item_id
            start_id = self.qid_dbxref[sub_qid]
            for s in item.statements:
                edge_lines.extend(self.handle_statement(s, start_id))

            node_template = self.parse_node(item)
            if node_template:
                node_lines.append(node_template.copy())

        self.edge_lines = edge_lines
        self.node_lines = node_lines

        self.write_out()


def main(mediawiki_api_url, sparql_endpoint_url, node_out_path, edge_out_path):
    bot = Bot(sparql_endpoint_url, mediawiki_api_url, node_out_path, edge_out_path)
    bot.run()


if __name__ == '__main__':
    p = configargparse.ArgParser(default_config_files=['config.cfg'])
    p.add('-c', '--config', is_config_file=True, help='config file path')
    p.add("--mediawiki_api_url", required=True, help="Wikibase mediawiki api url")
    p.add("--sparql_endpoint_url", required=True, help="Wikibase sparql endpoint url")
    p.add("--node-out-path", required=True, help="path to output neo4j nodes csv")
    p.add("--edge-out-path", required=True, help="path to output neo4j edges csv")
    options, _ = p.parse_known_args()
    print(options)
    d = options.__dict__.copy()
    del d['config']
    main(**d)
