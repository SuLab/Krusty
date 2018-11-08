"""
Many options to get the data out

this uses the wikimedia API, to load the statements up in WDI

Alternative option: dumpRdf.php in /var/www/html/extensions/Wikibase/repo/maintenance
then load it in robot or rdflib

"""
import pandas as pd
from tqdm import tqdm
from wikidataintegrator import wdi_core, wdi_helpers
from more_itertools import chunked

pd.options.display.width = 200
pd.set_option("display.max_column", 12)

HOST = "100.25.145.12"
WIKIBASE_PORT = "8181"
WDQS_PORT = "8282"
mediawiki_api_url = "http://{}:{}/w/api.php".format(HOST, WIKIBASE_PORT)
sparql_endpoint_url = "http://{}:{}/proxy/wdqs/bigdata/namespace/wdq/sparql".format(HOST, WDQS_PORT)

uri_pid = wdi_helpers.id_mapper("P2", endpoint=sparql_endpoint_url)
pid_uri = {v: k for k, v in uri_pid.items()}
dbxref_pid = uri_pid['http://www.geneontology.org/formats/oboInOwl#DbXref']
dbxref_qid = wdi_helpers.id_mapper(dbxref_pid, endpoint=sparql_endpoint_url)
qid_dbxref = {v: k for k, v in dbxref_qid.items()}
ref_supp_text_pid = uri_pid["http://reference_supporting_text"]
reference_uri_pid = uri_pid["http://www.wikidata.org/entity/P854"]
instance_of_pid = uri_pid["http://www.w3.org/1999/02/22-rdf-syntax-ns#rdf_type"]

# prop label and descriptions
pids = {x for x in qid_dbxref if x.startswith("P")}
props = wdi_core.WDItemEngine.generate_item_instances(list(pids), mediawiki_api_url)
pid_label = {pid: item.get_label() for pid, item in props}

# get all items and all statements
items = []
qids = {x for x in qid_dbxref if x.startswith("Q")}
"""
# take too much ram
for chunk in tqdm(chunked(qids, 50), total=len(qids) / 50):
    items.extend(wdi_core.WDItemEngine.generate_item_instances(list(chunk), mediawiki_api_url))
"""
### edges
edge_columns = [':START_ID', ':TYPE', ':END_ID', 'reference_uri', 'reference_supporting_text',
                'reference_date', 'property_label', 'property_description:IGNORE', 'property_uri']
edge_template = {x: "NA" for x in edge_columns}
lines = []
for sub_qid in tqdm(qids):
    # sub_qid = "Q1513"
    item = wdi_core.WDItemEngine(wd_item_id=sub_qid, mediawiki_api_url=mediawiki_api_url)
    edge_template[':START_ID'] = qid_dbxref[sub_qid]

    for s in item.statements:
        line = edge_template.copy()
        line['property_uri'] = pid_uri[s.get_prop_nr()]
        line['property_label'] = pid_label[s.get_prop_nr()]
        line[':TYPE'] = qid_dbxref[s.get_prop_nr()]
        if line['property_uri'] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#rdf_type":
            continue
        if line['property_uri'] == "http://www.geneontology.org/formats/oboInOwl#DbXref":
            continue
        line[':END_ID'] = qid_dbxref["Q" + str(s.get_value())] if s.data_type == "wikibase-item" else s.get_value()
        if s.references:
            for ref in s.references:
                ref_supp_text_statements = [x for x in ref if x.get_prop_nr() == ref_supp_text_pid]
                ref_supp_text = " ".join([x.get_value() for x in ref_supp_text_statements])
                reference_uri_statements = [x for x in ref if x.get_prop_nr() == reference_uri_pid]
                reference_uri = "|".join([x.get_value() for x in reference_uri_statements])
                # todo: rejoin split pubmed urls
                line['reference_supporting_text'] = ref_supp_text
                line['reference_uri'] = reference_uri
                lines.append(line.copy())
        else:
            lines.append(line.copy())

df_edges = pd.DataFrame(lines)
df_edges = df_edges[edge_columns]
df_edges.to_csv("edges.csv")

### nodes
node_columns = ['id:ID', ':LABEL', 'preflabel', 'synonyms:IGNORE', 'name', 'description']
lines = []
for qid, item in tqdm(items):
    type_statements = [s for s in item.statements if s.get_prop_nr() == instance_of_pid]
    if len(type_statements) != 1:
        continue
    node_template = dict()
    node_template[':LABEL'] = qid_dbxref["Q" + str(type_statements[0].get_value())]
    node_template['id:ID'] = qid_dbxref[qid]
    node_template['preflabel'] = item.get_label()
    node_template['name'] = item.get_label()
    node_template['description'] = item.get_description()
    node_template['synonyms:IGNORE'] = "|".join(item.get_aliases())
    lines.append(node_template.copy())
df_nodes = pd.DataFrame(lines)
df_nodes = df_nodes[node_columns]
df_nodes.to_csv("nodes.csv")
