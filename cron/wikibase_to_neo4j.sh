## This is the cron job to upload wikibase to Neo4j
echo "This is the cron job to upload wikibase into Neo4j [start_time: $(date +%F_%H-%M-%S)]."
echo ""

# neo4j upload if the new graph has been dumped out
neo4j=~/neo4j-wikibase
nodes=$neo4j/import/concepts.csv
edges=$neo4j/import/statements.csv
if [[ -f $nodes && -f $edges ]]; then
   echo "New graph dump successfully imported."
   echo "Uploading the dump into Neo4j..."
   echo ""
   $neo4j/bin/neo4j stop
   mkdir -p $neo4j/data/databases/graph.db
   rm -rf $neo4j/data/databases/graph.db/*
   $neo4j/bin/neo4j-admin import --id-type string --nodes $nodes --relationships $edges --ignore-missing-nodes --ignore-duplicate-nodes
   cd $neo4j/data/databases/graph.db
   $neo4j/bin/neo4j start
   echo ""
   echo "Neo4j upload completed."
   echo ""
   # mv imported network
   mv $nodes $neo4j/import/concepts_uploaded.csv
   mv $edges $neo4j/import/statements_uploaded.csv
   gzip -f --best $neo4j/import/concepts_uploaded.csv
   gzip -f --best $neo4j/import/statements_uploaded.csv
   echo "Renamed network files done. The new graph is up in the web browser. Bye."
else
   echo "New graph did not dump out successfully. Exit."
fi

echo ""
echo "Exitting the cron job to upload wikibase into Neo4j [end_time: $(date +%F_%H-%M-%S)]."
echo "..................................................................................................................."
