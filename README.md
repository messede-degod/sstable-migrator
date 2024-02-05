### sstable-migrator

# Building
 -  install and use **java 8**, check with `java -version`
 -  compile - `mvn compile`
 -  run - `mvn exec:java` to convert `input/*` to sstables in `/output`

# Setup Cassandra
 - Start Container - `sudo docker run -v ./output/:/ferret/dnsdata  -d --name cassandra --hostname cassandra --network cassandra cassandra` (Allow upto a minute for bootup)
 - Start a cqlsh shell - `sudo docker exec -it cassandra cqlsh`
 - Create Keyspace - `CREATE KEYSPACE ferret WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};`
 - Create Table - `CREATE TABLE ferret.dnsdata (apexDomain VARCHAR,recordType VARCHAR, subDomain VARCHAR, ipAddress INET, country VARCHAR, city VARCHAR, asn VARCHAR, as_name VARCHAR,tld VARCHAR PRIMARY KEY (ipAddress,apexDomain,subDomain,tld) );`
 - Move Data - `sudo docker container exec -it cassandra  sstableloader -d 172.18.0.2 /ferret/dnsdata/`

# Possible Improvements
 -  <strike>use java FileChannel to read files (possible performance improvements)</strike> (no improvements observed)
 -  use fastjson parser
 -  <strike>use multithreaded writes to CQLSSTableWriter (https://issues.apache.org/jira/browse/CASSANDRA-7463)</strike> (bad idea, write performance is far better when keys are in order, writes with out of order keys take up a lot of cpu, but yield no improvement in conversion time)
