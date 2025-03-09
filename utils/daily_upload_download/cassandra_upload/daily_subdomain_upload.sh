WORK_DIR="/nonsec/sstable-migrator/"
CASSANDRA_BIN_HOME="../apache-cassandra-4.1.3/bin"

cd $WORK_DIR

#####################################################################################

mkdir -p csv_input
mkdir -p old_csv_input

mv csv_input/* old_csv_input/ 2>/dev/null

timestamp=$(date -d "2 day ago" +%Y-%m-%d)

wget https://cs1.ip.thc.org/${timestamp}.txt.gz -O csv_input/${timestamp}.txt.gz
if [ $? -ne 0 ]; then
    wget https://cs2.ip.thc.org/${timestamp}.txt -O csv_input/${timestamp}.txt.gz
    if [ $? -ne 0 ]; then
        exit 11
    fi
fi

# extract archive
gzip -d csv_input/${timestamp}.txt.gz

# remove unwanted domains
grep -v -E -f unwanted-domains csv_input/${timestamp}.txt > sa
mv sa csv_input/${timestamp}.txt

# remove *, ex: *.abc.com is converted to abc.com
cat csv_input/${timestamp}.txt | sed -E 's/^\*\.//gm;t' > sa
mv sa csv_input/${timestamp}.txt


rm -r output/*

mvn exec:java -DargLine="-Xms6144m  -Xmx8144m" -Dexec.args="CSV_SUBD" | tee sstable-migrator.log

$CASSANDRA_BIN_HOME/sstableloader -d 127.0.0.1 ferret/subdomains

mv csv_input/* old_domains/
