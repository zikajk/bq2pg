#!/bin/bash

cd "$(dirname "$0")"
lein uberjar
cp ./target/bq2pg-1.0.0-standalone.jar ~/bin/bq2pg.jar
echo "#!/bin/bash" > ~/bin/bq2pg
echo "java -jar ~/bin/bq2pg.jar \$1" >> ~/bin/bq2pg
chmod 755 ~/bin/bq2pg
