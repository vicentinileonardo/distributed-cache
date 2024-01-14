# distributed-cache
Distributed Systems 1 course project 

# HTTP Server endpoints

The majority of the following are all GET requests, even if it is not completely RESTful compliant.
Names of clients, caches and database are of the type: $a, $b, $c

The only POST request is the one that is used to start operations for a client.

http://localhost:3003/clients
http://localhost:3003/clients/{client_name}   //to get list of operations for a client

http://localhost:3003/l2caches
http://localhost:3003/l2caches/{cache_name}/state
http://localhost:3003/l2caches/{cache_name}/crash
http://localhost:3003/l2caches/{cache_name}/recover

http://localhost:3003/l1caches
http://localhost:3003/l1caches/{cache_name}/state
http://localhost:3003/l1caches/{cache_name}/crash
http://localhost:3003/l1caches/{cache_name}/recover

http://localhost:3003/db/state

http://localhost:3003/healthCheck

This endpoint assumes no crashes, so before using it,  you need to make sure all caches are up and running.

POST http://localhost:3003/clients/{client_name}

Body
{
    "operation": "crit_write",
    "key": "5",
    "value": 995
}

{client_name} could also be the string "random"