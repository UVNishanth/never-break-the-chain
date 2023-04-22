Data storage using a chain-replication system

A hashmap is used as the underlying datastore and we provide two functionalities:  
<code>get(key) - to fetch the value of the key from the hashmap</code><br>
<code>increment(key, incVal) - increment the value of key by incVal</code>  

System is concurrency-resilient and also heals the chain when a node goes down or a new one is added. It leverages Zookeeper to maintain the chain. The chain nodes are added to a `control_path` node on Zookeeper

The following line adds a node running at {grpc_host_and_port} to the chain <br>
<code>java -jar chainreplication.jar your_name grpc_host_port zookeeper_host_port_list control_path</code>

Protobuf files have been provided to build a client. <code>chain.proto</code> gives the request structure to access the chain for data. <code>chain_debug.proto</code> provides debugging requests to the chain to get a snapshot of the hashmap at any given time or to kill a server in the chain.
