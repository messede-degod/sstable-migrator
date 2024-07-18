# Host were daily processed certstreams are available 
DATA_DOMAIN="data.localdomain"

# Set Telegram API token and chat ID
API_TOKEN=""
CHAT_ID=""

WORK_DIR="/nonsec/sstable-migrator/"
CASSANDRA_BIN_HOME="../apache-cassandra-4.1.3/bin"

###################################################

cd $WORK_DIR
echo "Started on $(date --iso-8601=seconds)" >> run.logs

mkdir -p old_csv_input
mkdir -p csv_input

mv csv_input/* old_csv_input/ 2>/dev/null

# get latest massdns output

timestamp=$(date --iso-8601)

echo "[+] Downloading data for $timestamp"

# get data from daily data server
wget  http://${data_domain}/output/output_${timestamp}.txt -O csv_input/domains_${timestamp}.csv
if [ $? -ne 0 ]; then
    exit 11
fi

rm -r output/*

mvn exec:java -DargLine="-Xms6144m  -Xmx8144m"  | tee sstable-migrator.log

$CASSANDRA_BIN_HOME/sstableloader -d 127.0.0.1 ferret/dnsdata
$CASSANDRA_BIN_HOME/sstableloader -d 127.0.0.1 ferret/cnames
$CASSANDRA_BIN_HOME/sstableloader -d 127.0.0.1 ferret/subdomains


MESSAGE="SSTable-Migrator %0A$(wc -l csv_input/domains_${timestamp}.csv| cut -f1 -d' ') records have been successfully imported into cassandra"

curl -s -X POST https://api.telegram.org/bot$API_TOKEN/sendMessage -d chat_id=$CHAT_ID -d text="$MESSAGE"
