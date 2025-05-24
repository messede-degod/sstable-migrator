# Download and extract domains
if [ ! -f clean_domains ]; then
    go build convert.go
fi
time cat cc-index.paths  | while read x; do bash ./download_and_convert.sh $x; done

# Sort domains
sort -u domains > a
mv a domains

# Clean domains
if [ ! -f clean_domains ]; then
    go build clean_domains.go
fi
cat domains | ./clean_domains > a
mv a domains