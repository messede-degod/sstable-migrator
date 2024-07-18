#download common crawl data and extact domains

wget https://ds5q9oxwqwsfj.cloudfront.net/$1
filename=$(echo $1 | cut -f5 -d'/' | cut -f1 -d'.')
echo "[+] gzip decompress...."
gzip -d $filename.gz
echo "[+] picking urls"
cut -f1 -d')' $filename > urls
echo "[+] sorting urls"
sort -u urls > a
echo "[+] converting "
./convert a b
cat b >> domains

rm urls
rm a
rm b
rm $filename