#  Multi-level distributed cache

Distributed Systems 1 course project: Multi-level distributed cache

## How to run

The project can be run using the following command:
```bash
gradle run
```

To stop the system, simply press CTRL+C

## How to interact with the system

The system can be interacted with code, inside the main method of the DistributedCacheSystem class.

The system can also be interacted with using the HTTP server, which is started by default on port 3003.

To crash and recover caches, you can use either the endpoints or the crash() and recover() methods strategically placed in the code.
If needed, you can also add arbitrary delays in the code: the addDelayInSeconds() methods is placed on every function dealing with a request.
Arbitrary delays could be also added to the database actor, to ease the testing of the system (to gain time to manually crash a cache).

Number of actors can be tuned in the config.yaml file.
Timeouts can be tuned in the config.yaml file.
If timeouts are not set carefully, the system might not work properly: protocols might behave differently than expected.


## HTTP Server endpoints

The majority of the following are all GET requests, even if it is not completely RESTful compliant.
Names of clients, caches and database are of the type: $a, $b, $c
With the standard default configuration, there are 5 clients, 5 L2 caches, 3 L1 caches and 1 database.
Database is called by Akka: $a
L1 caches are called by Akka: $b, $c, $d
L2 caches are called by Akka: $e, $f, $g, $h, $i
Clients are called by Akka: $j, $k, $l, $m, $n

The only POST request is the one that is used to start operations for a client.

http://localhost:3003/clients
http://localhost:3003/clients/{client_name}   // to get list of operations for a client

http://localhost:3003/l2caches
http://localhost:3003/l2caches/{cache_name}/state
http://localhost:3003/l2caches/{cache_name}/crash
http://localhost:3003/l2caches/{cache_name}/recover

http://localhost:3003/l1caches
http://localhost:3003/l1caches/{cache_name}/state
http://localhost:3003/l1caches/{cache_name}/crash
http://localhost:3003/l1caches/{cache_name}/recover

http://localhost:3003/db/state

http://localhost:3003/healthCheck   // to check for system consistency
The "healthCheck" endpoint assumes no crashes, so before using it, you need to make sure all caches are up and running.
If a cache is crashed, you need to recover it before using the healthCheck endpoint.


POST http://localhost:3003/clients/{client_name}

Body
{
    "operation": "crit_write",
    "key": "5",
    "value": 995
}

{client_name} could also be the string "random"