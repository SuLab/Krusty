Notes:

To increase label, description, alias string length limit
```
docker exec -it wikibase_wikibase_1 /bin/bash
nano /var/www/html/extensions/Wikibase/repo/config/Wikibase.default.php
# change the following line from 250 to whatever you want
# 'multilang-limits' => ['length' => 250],
```