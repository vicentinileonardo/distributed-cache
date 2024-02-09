package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import it.unitn.ds1.Message.*;

import static akka.http.javadsl.server.PathMatchers.integerSegment;
import static akka.http.javadsl.server.PathMatchers.segment;

import akka.http.javadsl.marshallers.jackson.Jackson;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class HTTPRoutes extends AllDirectives {

    private String classString = String.valueOf(getClass());

    private ArrayNode getArrayNode(HashSet<ActorRef> actors) {
        ArrayNode array = JsonNodeFactory.instance.arrayNode();
        for (ActorRef actor : actors) {
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            node.put("id", actor.path().name());
            array.add(node);
        }
        return array;
    }

    private ObjectNode createResponse(DistributedCacheSystem system, String type){
        ObjectNode response = JsonNodeFactory.instance.objectNode();

        int size = 0;
        switch (type){
            case "clients":
                size = system.getClients().size();
                ArrayNode clientsArray = getArrayNode(system.getClients());
                response.set("size", JsonNodeFactory.instance.numberNode(size));
                response.set("clients", clientsArray);
                break;
            case "l2caches":
                size = system.getL2Caches().size();
                ArrayNode l2cachesArray = getArrayNode(system.getL2Caches());
                response.set("size", JsonNodeFactory.instance.numberNode(size));
                response.set("l2caches", l2cachesArray);
                break;
            case "l1caches":
                size = system.getL1Caches().size();
                ArrayNode l1cachesArray = getArrayNode(system.getL1Caches());
                response.set("size", JsonNodeFactory.instance.numberNode(size));
                response.set("l1caches", l1cachesArray);
                break;
            default:
                break;
        }

        return response;
    }

    public Route getClients(DistributedCacheSystem system) {
        return path("clients", () ->
            get(() -> {
                ObjectNode response = createResponse(system, "clients");
                return completeOK(response, Jackson.marshaller());
            })
        );
    }

    public Route getL2Caches(DistributedCacheSystem system) {
        return path("l2caches", () ->
            get(() -> {
                ObjectNode response = createResponse(system, "l2caches");
                return completeOK(response, Jackson.marshaller());
            })
        );
    }

    public Route getL1Caches(DistributedCacheSystem system) {
        return path("l1caches", () ->
            get(() -> {
                ObjectNode response = createResponse(system, "l1caches");
                return completeOK(response, Jackson.marshaller());
            })
        );
    }


    public static class Payload {
        public String operation;
        public Integer key;
        public Integer value;
    }

    public Route clientOperations(DistributedCacheSystem system) {
        return path(segment("clients").slash(segment()), (String id) ->
                post(() ->
                        entity(Jackson.unmarshaller(Payload.class), payload -> {

                            if (payload.operation == null) {
                                ObjectNode message = JsonNodeFactory.instance.objectNode();
                                String value = "Operation not found";
                                message.put("message", value);
                                return completeOK(message, Jackson.marshaller());
                            }

                            if (payload.key == null) {
                                ObjectNode message = JsonNodeFactory.instance.objectNode();
                                String value = "Key not found";
                                message.put("message", value);
                                return completeOK(message, Jackson.marshaller());
                            }

                            HashSet<ActorRef> clients = system.getClients();

                            ActorRef foundClient = null;
                            if(!id.equals("random")){
                                //find the client with the given id
                                for (ActorRef client : clients) {
                                    if (client.path().name().equals(id)) {
                                        foundClient = client;
                                        break;
                                    }
                                }
                            } else {
                                //choose a random client
                                List<ActorRef> clientsList = new ArrayList<>(clients);
                                Random rand = new Random();
                                foundClient = clientsList.get(rand.nextInt(clientsList.size()));
                            }

                            if (foundClient == null) {
                                ObjectNode message = JsonNodeFactory.instance.objectNode();
                                String value = "Client: " + id + " not found";
                                message.put("message", value);
                                return completeOK(message, Jackson.marshaller());
                            }

                            switch (payload.operation) {
                                case "read":
                                    foundClient.tell(new StartReadRequestMsg(payload.key), ActorRef.noSender());
                                    break;
                                case "write":
                                    if (payload.value == null) {
                                        ObjectNode message = JsonNodeFactory.instance.objectNode();
                                        String value = "Value not found";
                                        message.put("message", value);
                                        return completeOK(message, Jackson.marshaller());
                                    }
                                    foundClient.tell(new StartWriteMsg(payload.key, payload.value), ActorRef.noSender());
                                    break;
                                case "crit_read":
                                    foundClient.tell(new StartCriticalReadRequestMsg(payload.key), ActorRef.noSender());
                                    break;
                                case "crit_write":
                                    if (payload.value == null) {
                                        ObjectNode message = JsonNodeFactory.instance.objectNode();
                                        String value = "Value not found";
                                        message.put("message", value);
                                        return completeOK(message, Jackson.marshaller());
                                    }
                                    foundClient.tell(new StartCriticalWriteRequestMsg(payload.key, payload.value), ActorRef.noSender());
                                    break;
                                default:
                                    ObjectNode message = JsonNodeFactory.instance.objectNode();
                                    String value = "Operation not supported";
                                    message.put("message", value);
                                    return completeOK(message, Jackson.marshaller());
                            }

                            // Finally, return a response
                            ObjectNode message = JsonNodeFactory.instance.objectNode();
                            if (payload.operation.equals("read") || payload.operation.equals("crit_read")) {
                                message.put("message", "Client: " + id + " contacted to perform " + payload.operation + " on key: " + payload.key);
                            } else {
                                message.put("message", "Client: " + id + " contacted to perform " + payload.operation + " on key: " + payload.key + " with value: " + payload.value);
                            }
                            return completeOK(message, Jackson.marshaller());
                        })
                )
        );
    }

    public Route clientOperationsList(DistributedCacheSystem system) {
        return path(segment("clients").slash(segment()), (String id) ->
                get(() -> {
                    CustomPrint.print(classString, "", "", "Trying to get operation list of client: " + id );

                    HashSet<ActorRef> clients = system.getClients();

                    ActorRef foundClient = null;

                    //find the client with the given id
                    for (ActorRef client : clients) {
                        if (client.path().name().equals(id)) {
                            foundClient = client;
                            break;
                        }
                    }

                    if (foundClient == null) {
                        ObjectNode message = JsonNodeFactory.instance.objectNode();
                        String value = "Client: " + id + " not found";
                        message.put("message", value);
                        return completeOK(message, Jackson.marshaller());
                    }

                    foundClient.tell(new ClientOperationsListMsg(), ActorRef.noSender());
                    ObjectNode message = JsonNodeFactory.instance.objectNode();
                    String value = "ClientOperationsList message sent to client: " + id;
                    CustomPrint.print(classString, "", "", value);
                    message.put("message", value);
                    return completeOK(message, Jackson.marshaller());
                })
        );
    }

    public Route crashL2caches(DistributedCacheSystem system) {
        return path(segment("l2caches").slash(segment()).slash("crash"), (String id) ->
            get(() -> {
                CustomPrint.print(classString, "", "", "Trying to CRASH L2 cache: " + id );

                HashSet<ActorRef> l2Caches = system.getL2Caches();
                //find the l2 cache with the given id
                ActorRef l2Cache = null;
                for (ActorRef l2 : l2Caches) {
                    if (l2.path().name().equals(id)) {
                        l2Cache = l2;
                        break;
                    }
                }
                if (l2Cache == null) {
                    ObjectNode message = JsonNodeFactory.instance.objectNode();
                    String value = "L2 cache: " + id + " not found";
                    CustomPrint.print(classString, "", "", value);
                    message.put("message", value);
                    return completeOK(message, Jackson.marshaller());
                }
                l2Cache.tell(new CrashMsg(), ActorRef.noSender());
                ObjectNode message = JsonNodeFactory.instance.objectNode();
                String value = "Crash message sent to L2 cache: " + id;
                CustomPrint.print(classString, "", "", value);
                message.put("message", value);
                return completeOK(message, Jackson.marshaller());
            })
        );
    }

    public Route crashL1caches(DistributedCacheSystem system) {
        return path(segment("l1caches").slash(segment()).slash("crash"), (String id) ->
            get(() -> {
                CustomPrint.print(classString, "", "", "Trying to CRASH L1 cache: " + id );

                HashSet<ActorRef> l1Caches = system.getL1Caches();
                //find the l1 cache with the given id
                ActorRef l1Cache = null;
                for (ActorRef l1 : l1Caches) {
                    if (l1.path().name().equals(id)) {
                        l1Cache = l1;
                        break;
                    }
                }
                if (l1Cache == null) {
                    ObjectNode message = JsonNodeFactory.instance.objectNode();
                    String value = "L1 cache: " + id + " not found";
                    CustomPrint.print(classString, "", "", value);
                    message.put("message", value);
                    return completeOK(message, Jackson.marshaller());
                }
                l1Cache.tell(new CrashMsg(), ActorRef.noSender());
                ObjectNode message = JsonNodeFactory.instance.objectNode();
                String value = "Crash message sent to L1 cache: " + id;
                CustomPrint.print(classString, "", "", value);
                message.put("message", value);
                return completeOK(message, Jackson.marshaller());
            })
        );
    }

    public Route recoverL2caches(DistributedCacheSystem system) {
        return path(segment("l2caches").slash(segment()).slash("recover"), (String id) ->
            get(() -> {
                CustomPrint.print(classString, "", "", "Trying to RECOVER L2 cache: " + id );

                HashSet<ActorRef> l2Caches = system.getL2Caches();
                //find the l2 cache with the given id
                ActorRef l2Cache = null;
                for (ActorRef l2 : l2Caches) {
                    if (l2.path().name().equals(id)) {
                        l2Cache = l2;
                        break;
                    }
                }
                if (l2Cache == null) {
                    ObjectNode message = JsonNodeFactory.instance.objectNode();
                    String value = "L2 cache: " + id + " not found";
                    CustomPrint.print(classString, "", "", value);
                    message.put("message", value);
                    return completeOK(message, Jackson.marshaller());
                }
                l2Cache.tell(new RecoverMsg(), ActorRef.noSender());
                ObjectNode message = JsonNodeFactory.instance.objectNode();
                String value = "Recovery message sent to L2 cache: " + id;
                CustomPrint.print(classString, "", "", value);
                message.put("message", value);
                return completeOK(message, Jackson.marshaller());
            })
        );
    }

    public Route recoverL1caches(DistributedCacheSystem system) {
        return path(segment("l1caches").slash(segment()).slash("recover"), (String id) ->
            get(() -> {
                CustomPrint.print(classString, "", "", "Trying to RECOVER L1 cache: " + id );

                HashSet<ActorRef> l1Caches = system.getL1Caches();
                //find the l1 cache with the given id
                ActorRef l1Cache = null;
                for (ActorRef l1 : l1Caches) {
                    if (l1.path().name().equals(id)) {
                        l1Cache = l1;
                        break;
                    }
                }
                if (l1Cache == null) {
                    ObjectNode message = JsonNodeFactory.instance.objectNode();
                    String value = "L1 cache: " + id + " not found";
                    CustomPrint.print(classString, "", "", value);
                    message.put("message", value);
                    return completeOK(message, Jackson.marshaller());
                }
                l1Cache.tell(new RecoverMsg(), ActorRef.noSender());
                ObjectNode message = JsonNodeFactory.instance.objectNode();
                String value = "Recovery message sent to L1 cache: " + id;
                CustomPrint.print(classString, "", "", value);
                message.put("message", value);
                return completeOK(message, Jackson.marshaller());
            })
        );
    }

    public Route stateL1caches(DistributedCacheSystem system) {
        return path(segment("l1caches").slash(segment()).slash("state"), (String id) ->
                get(() -> {
                    CustomPrint.print(classString, "", "", "Trying to print STATE of L1 cache: " + id );

                    HashSet<ActorRef> l1Caches = system.getL1Caches();
                    //find the l1 cache with the given id
                    ActorRef l1Cache = null;
                    for (ActorRef l1 : l1Caches) {
                        if (l1.path().name().equals(id)) {
                            l1Cache = l1;
                            break;
                        }
                    }
                    if (l1Cache == null) {
                        ObjectNode message = JsonNodeFactory.instance.objectNode();
                        String value = "L1 cache: " + id + " not found";
                        CustomPrint.print(classString, "", "", value);
                        message.put("message", value);
                        return completeOK(message, Jackson.marshaller());
                    }
                    l1Cache.tell(new InfoItemsMsg(), ActorRef.noSender());
                    ObjectNode message = JsonNodeFactory.instance.objectNode();
                    String value = "InfoItems message sent to L1 cache: " + id;
                    CustomPrint.print(classString, "", "", value);
                    message.put("message", value);
                    return completeOK(message, Jackson.marshaller());
                })
        );
    }

    public Route stateL2caches(DistributedCacheSystem system) {
        return path(segment("l2caches").slash(segment()).slash("state"), (String id) ->
                get(() -> {
                    CustomPrint.print(classString, "", "", "Trying to print STATE of L2 cache: " + id );

                    HashSet<ActorRef> l2Caches = system.getL2Caches();
                    //find the l2 cache with the given id
                    ActorRef l2Cache = null;
                    for (ActorRef l2 : l2Caches) {
                        if (l2.path().name().equals(id)) {
                            l2Cache = l2;
                            break;
                        }
                    }
                    if (l2Cache == null) {
                        ObjectNode message = JsonNodeFactory.instance.objectNode();
                        String value = "L2 cache: " + id + " not found";
                        CustomPrint.print(classString, "", "", value);
                        message.put("message", value);
                        return completeOK(message, Jackson.marshaller());
                    }
                    l2Cache.tell(new InfoItemsMsg(), ActorRef.noSender());
                    ObjectNode message = JsonNodeFactory.instance.objectNode();
                    String value = "InfoItems message sent to L2 cache: " + id;
                    CustomPrint.print(classString, "", "", value);
                    message.put("message", value);
                    return completeOK(message, Jackson.marshaller());
                })
        );
    }

    public Route stateDB(DistributedCacheSystem system) {
        return path(segment("db").slash("state"), () ->
                get(() -> {
                    CustomPrint.print(classString, "", "", "Trying to print STATE of DB");

                    system.getDatabase().tell(new CurrentDataMsg(), ActorRef.noSender());

                    ObjectNode message = JsonNodeFactory.instance.objectNode();
                    String value = "CurrentData message sent to DB";
                    CustomPrint.print(classString, "", "", value);
                    message.put("message", value);
                    return completeOK(message, Jackson.marshaller());
                })
        );
    }

    public Route consistencyCheck(DistributedCacheSystem system) {
        return path("healthCheck", () ->
                get(() -> {
                    CustomPrint.print(classString, "", "", "Starting consistency check" );

                    //find the l2 cache with the given id
                    ActorRef master = system.getMaster();

                    master.tell(new StartHealthCheck(), ActorRef.noSender());

                    ObjectNode message = JsonNodeFactory.instance.objectNode();
                    String value = "Consistency check requested";
                    CustomPrint.print(classString, "", "", value);
                    message.put("message", value);
                    return completeOK(message, Jackson.marshaller());
                })
        );
    }


}

