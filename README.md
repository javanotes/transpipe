# transpipe

A spring boot starter project for a microservice transaction orchestration platform over Kafka. The implementation does not use classic 2PC protocol, instead a cascading message passing pattern more sutiable for an eventual consistent architecture. A TRANSaction PIPEline.

```
     ---<-- Commit   ()////)==============| execute_commit
     |                                    | TXN_1
 ----|----> Rollback ()////)==============| execute_rollback
|    |
|    |
|    |-----------------------|
|                            V
|                          Commit   ()////)===========| execute_commit
|                                                     | TXN_2
|------------<-----------  Rollback ()////)===========| execute_rollback
```

Every transaction (service) has 2 listeners - a commit listener, and a rollback listener. Based on the trigger received, it will execute commit/rollback. On successful commit execution, a trigger is passed to the next (downstream) transaction (via its commit link). On rollback execution, upon commit failure, a trigegr is passed to the previous (upstream) transaction (via its rollback link).

Thus transactional triggers are cascaded as if in a 2 way linked list, using the term very loosely. The orchestration pipeline need to defined as a sequence of such transaction service identifiers. The commit and rollback links will be handled by the platform as Kafka topics, internally. Thus all message passing will be through Kafka topics, and the participating services themselves will be oblivious of its sibling services. 

The services can be scaled horizontally (considering the Kafka topic partitions) and they can come and go. The service container will additionally expose REST api to orchestrate action commands. An edge service can act as the orchestration manager, providing an endpoint facade for the microservice cluster.
