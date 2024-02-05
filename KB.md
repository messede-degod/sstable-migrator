## Limiting per parition

- Limit one result per apexDomain
    -   SELECT * FROM ferret.dnsdata PER PARTITION LIMIT 1;


## Pagination
Cassandra doesn't allow traditional OFFSET LIMITS.
-   page state must be obtained for queries
-   further queries can be continued from the previous page state
-   this obviously prevents random paging.