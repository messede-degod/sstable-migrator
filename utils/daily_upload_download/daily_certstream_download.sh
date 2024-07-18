## download and convert cerstream data

WORK_DIR="/root/dnsdata"
PUREDNS_BIN="/root/go/bin/puredns"
MASSDNS_BIN="/usr/local/bin/massdns"

# Set Telegram API token and chat ID
API_TOKEN=""
CHAT_ID=""

cd $WORK_DIR

JOBID=$RANDOM

echo "JOBID:$JOBID Started on $(date --iso-8601=seconds) $(pwd)" >> run.logs

mkdir -p domains
mkdir -p massdns
mkdir -p output
mkdir -p raw_certstream

touch additional_domains.txt # list any additional domains that you want to process here

echo "[+] Downloading Domains List..."
# Download daily dump

/usr/bin/wget https://pub.ajam.dev/datasets/certstream/all_latest.txt -O all_latest.txt
/usr/bin/wget https://pub.ajam.dev/datasets/certstream/nrd_latest.txt -O nrd_latest.txt

mv nrd_latest.txt domains.txt

cat additional_domains.txt >> domains.txt
truncate -s 0 additional_domains.txt

rx='\.('
rx+='gov\.[a-z]{2,}$|'
rx+='gov$|'
rx+='ru$|'
rx+='ai$|'
rx+='de$|'
rx+='fr$|'
rx+='io$|'
rx+='in$)' ## LAST entry

grep -E "$rx" all_latest.txt  >> domains.txt

rm *_latest.txt


# sort everything, so that we can check for dups
sort -u domains.txt > a
mv a domains.txt

echo "domains.txt" $(wc -l domains.txt)


# duplicate entries from the last 7 days
remove_last_20_days() {

    today=$(date +%Y-%m-%d)

    for i in {20..1}; do
        date=$(date -d "$i day ago" +%Y-%m-%d)

        if [ -e "domains/domains_$date.txt" ]; then
            comm -23 domains.txt domains/domains_$date.txt > a
            mv a domains.txt
            echo "[Removed Dups For $date] -  domains.txt now has $(wc -l domains.txt | cut -f1 -d' ') records"
        else
            echo "Did not find - domains/domains_$date.txt for dup check"
        fi
    done
}


remove_last_20_days


echo "[+] Starting Resolution..."


timestamp=$(date --iso-8601)

# https://github.com/d3mondev/puredns/
$PUREDNS_BIN --bin $MASSDNS_BIN  resolve domains.txt --rate-limit 1000  --skip-wildcard-filter --write-massdns  massdns_${timestamp}.txt

sed -i '/^$/d' massdns_${timestamp}.txt

tr ' ' ',' < massdns_${timestamp}.txt > output_${timestamp}.txt


mv output_${timestamp}.txt output/output_${timestamp}.txt
mv massdns_${timestamp}.txt massdns/massdns_${timestamp}.txt
mv domains.txt domains/domains_${timestamp}.txt


cuniq=$(wc -l domains/domains_${timestamp}.txt | cut -f1 -d' ')
obtained_dns_records=$(wc -l output/output_${timestamp}.txt| cut -f1 -d' ')


MESSAGE="New Data Obtained! %0ACertstream Unique: $cuniq %0ATotalRecords: $obtained_dns_records"

/usr/bin/curl -s -X POST https://api.telegram.org/bot$API_TOKEN/sendMessage -d chat_id=$CHAT_ID -d text="$MESSAGE"


echo "JOBID:$JOBID Ended on $(date --iso-8601=seconds) $(pwd)" >> run.logs
