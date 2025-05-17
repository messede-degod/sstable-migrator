### Make Table
```
CREATE TABLE domains
(
    domain String,
    apexdomain String,
    tld String
)
ENGINE = MergeTree
ORDER BY domain;
```

### Importing Data
```
INSERT INTO domains(domain)
SELECT * FROM url(
    'https://cs2.ip.thc.org/2025-05-16.txt.gz')
```


### Populate ApexDomain
```
ALTER TABLE domains
UPDATE apexdomain = arrayStringConcat(arraySlice(splitByString('.', domain), -2, 2), '.')
WHERE 1=1;
```

### Populate TLD
```
ALTER TABLE domains
UPDATE tld = arrayStringConcat(arraySlice(splitByString('.', domain), -2, 1), '.')
WHERE 1=1;
```

### Query ApexDomain Stats
```
select apexdomain,count(*) 
from 
domains 
group by apexdomain
order by count(*) desc
limit 100;
```  