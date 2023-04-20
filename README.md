# pass-by-chain
Data storage using a chain-replication system

A hashmap is used as the underlying datastore and we provide two functionalities:  
<code>get(key) - to fetch the value of the key from the hashmap</code><br>
<code>increment(key, incVal) - increment the value of key by incVal</code>  

System is concurrency-resilient and also heals the chain when a node goes down or a new one is added

