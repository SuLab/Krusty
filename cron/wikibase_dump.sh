## This is the cron job to dump out wikibase and transfer it to aws
echo "This is the cron job to dump wikibase  [start_time: $(date +%F_%H-%M-%S)]."
echo ""

# wikibase dump out
# define variables
neo4j=~/workspace/comm2net/neo4j
nodes=$neo4j/import/wikibase/concepts.csv
edges=$neo4j/import/wikibase/statements.csv

# execute the dump out 
echo "Executing Krusty to dump out wikibase... [start_time: $(date +%F_%H-%M-%S)]"
source ~/python-environments/krusty/bin/activate
python3 ~/workspace/comm2net/Krusty/wd_to_neo4j.py --mediawiki_api_url http://ngly1graph.org:8181/w/api.php --sparql_endpoint_url http://ngly1graph.org:8282/proxy/wdqs/bigdata/namespace/wdq/sparql --node-out-path $nodes --edge-out-path $edges
deactivate
echo "Wikibase dump out completed. [end_time: $(date +%F_%H-%M-%S)]"
echo ""

# tranfer the dump files to the neo4j host server and back up the graph
echo "Checking the outcome of the dump process..."
if [[ -f $nodes && -f $edges ]]; then
   echo "Successful dump out. Transferring the dump files to the neo4j host server..."
   scp $nodes $edges aws:~/neo4j-wikibase/import
   echo "Transfer done."
   echo ""
   echo "Backing up the old graph..."

   mkdir -p $neo4j/import/wikibase/backup_v$(date +%F_%H)
   mv $nodes $neo4j/import/wikibase/backup_v$(date +%F_%H)/.
   mv $edges $neo4j/import/wikibase/backup_v$(date +%F_%H)/.
   tar -cf $neo4j/import/wikibase/backup_v$(date +%F_%H).tar $neo4j/import/wikibase/backup_v$(date +%F_%H) 
   gzip -f --best $neo4j/import/wikibase/backup_v$(date +%F_%H).tar
   rm -rf $neo4j/import/wikibase/backup_v$(date +%F_%H)

   echo "Graph back up done. Bye."
else 
   echo "New graph did not dump out successfully. Exit."
fi

echo ""
echo "Exitting the cron job to dump wikibase [end_time: $(date +%F_%H-%M-%S)]."
echo "...................................................................................................................."
