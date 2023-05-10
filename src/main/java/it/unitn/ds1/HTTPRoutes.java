package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;

import java.util.HashSet;
import java.util.List;

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
                l2Cache.tell(new Message.DummyMsg(42), ActorRef.noSender());
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
                l1Cache.tell(new Message.DummyMsg(42), ActorRef.noSender());
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
                l2Cache.tell(new Message.DummyMsg(24), ActorRef.noSender());
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
                l1Cache.tell(new Message.DummyMsg(24), ActorRef.noSender());
                ObjectNode message = JsonNodeFactory.instance.objectNode();
                String value = "Recovery message sent to L1 cache: " + id;
                CustomPrint.print(classString, "", "", value);
                message.put("message", value);
                return completeOK(message, Jackson.marshaller());
            })
        );
    }


}

