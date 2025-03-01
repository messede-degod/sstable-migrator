### sstable-migrator

# Building
 -  install and use **java 8**, check with `java -version`
 -  compile - `mvn compile`
 -  run - `MAVEN_OPTS="-Xmx7114M" mvn exec:java -DargLine="-Xms6144m  -Xmx7144m"` to convert `input/*` to sstables in `/output`

# Setup Cassandra
 - Start Container - `sudo docker run -v ./output/:/ferret/dnsdata  -d --name cassandra --hostname cassandra --network cassandra cassandra` (Allow upto a minute for bootup)
 - Start a cqlsh shell - `sudo docker exec -it cassandra cqlsh`
 
 - Create Keyspace - `CREATE KEYSPACE ferret WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};`
 
 - Create RDNS Table - `CREATE TABLE ferret.dnsdata (apexDomain VARCHAR,recordType VARCHAR, subDomain VARCHAR, ip8 INET, ip16 INET, ip24 INET, ipAddress INET, country VARCHAR, city VARCHAR, asn VARCHAR, as_name VARCHAR, PRIMARY KEY (ip8,ip16,ip24,ipAddress,tld,apexDomain,subDomain) );`

 - Create SubDomains table - `CREATE TABLE ferret.subdomains ( p1 VARCHAR, p2 VARCHAR, p3 VARCHAR, p4 VARCHAR, p5 VARCHAR, p6 VARCHAR, p7 VARCHAR, lastSeen date, PRIMARY KEY ((p1,p2,p3),p4,p5,p6,p7));`

 - Create CNAME table - `CREATE TABLE ferret.cnames ( target VARCHAR, apexDomain VARCHAR, domain VARCHAR, PRIMARY KEY (target,apexDomain,domain) );`
 
 - Move Data - `sudo docker container exec -it cassandra  sstableloader -d 172.18.0.2 /ferret/dnsdata/`

# Possible Improvements
 -  <strike>use java FileChannel to read files (possible performance improvements)</strike> (no improvements observed)
 -  use fastjson parser
 -  <strike>use multithreaded writes to CQLSSTableWriter (https://issues.apache.org/jira/browse/CASSANDRA-7463)</strike> (bad idea, write performance is far better when keys are in order, writes with out of order keys take up a lot of cpu, but yield no improvement in conversion time)


# TLD Source
 - https://data.iana.org/TLD/tlds-alpha-by-domain.txt