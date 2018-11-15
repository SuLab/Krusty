#### Wikibase Setup
- Follow instructions here: https://github.com/wmde/wikibase-docker/blob/master/README-compose.md
- Create a bot account in your wikibase (do it manually, or see [this](https://github.com/stuppie/wikibase-tools/blob/b0ba76f33f80c12da9ce51a1d2bcadbb4899033a/wikibase_tools/run.sh#L22))
- Change settings in config.cfg
- Run [this](https://github.com/stuppie/wikibase-tools/blob/master/wikibase_tools/initial_setup.py) to create an 'equivalent property' and 'equivalent class' property in the Wikibase, or make sure these properties have the appropriate URIs (`http://www.w3.org/2002/07/owl#equivalentProperty` and `http://www.w3.org/2002/07/owl#equivalentClass`).
- Increase label, description, alias string length limit (see Wikibase Setup Notes below)


### Neo4j to Wikidata Bot: neo4j_to_wd.py

For usage: neo4j_to_wd.py --help

#### File Formats
Expects nodes and edges csv with the following format

**nodes**

Expects the following column names:
id:ID,:LABEL,preflabel,synonyms:IGNORE,name,description
- "id:ID" is used as the canonical identifier and should be unique
- ":LABEL" is a string that is used as the "instance of" statement
- "prefLabel" is used as the item label
- "synonyms:IGNORE" and "name" are merged and used as the aliases. Pipe separated
- "description" is the description

**edges**

Expects the following column names:
:START_ID,:TYPE,:END_ID,reference_uri,reference_supporting_text,reference_date,property_label,property_description:IGNORE,property_uri
- ":START_ID" and ":END_ID": subj and obj of the edge. These should match an "id:ID" in the nodes file
- ":TYPE": property canonical ID
- "reference_uri": a pipe-separated ("|") string of reference URLs
- "reference_supporting_text": string
- "reference_date": ignored
- "property_label": used as the label for the property item in wikibase
- "property_description:IGNORE": used as the description for the property item in wikibase
- "property_uri": property uri. added as equivalent property statement

#### Notes
- Multiple rows in the edges file that consist of identical (start_id, property, end_id) will
be used to generate multiple references on the same statement.
- Multiple reference_uris within the same row will result in multiple reference urls on one reference.
- If a reference url is longer than 400 characters, it will truncated, unless it is a pubmed reference 
(i.e., it starts with "https://www.ncbi.nlm.nih.gov/pubmed/"). In that case, the pmids in the url
will be split among multiple reference url statements within the same reference.
- Reference urls starting with "ISBN-13" or "ISBN-10" are handled specially. If the reference
url is not a URL (besides those isbns), it will fail.

### Wikibase Setup Notes

To increase label, description, alias string length limit
```
ID=$(docker-compose ps -q wikibase)
docker exec -it $ID /bin/bash
nano /var/www/html/extensions/Wikibase/repo/config/Wikibase.default.php
# change the following line from 250 to whatever you want
# 'multilang-limits' => ['length' => 250],
```

### Wikidata to Neo4j Bot: wd_to_neo4j.py

Write out all item and statements in the Wikibase to a nodes and edges file in the format described above

The only thing that will be lossy is if a reference url was truncated.

For usage: wd_to_neo4j.py --help
