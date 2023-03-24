package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Props;

// import java.io.Serializable;
import java.util.*;

public class Cache extends AbstractActor{

    private enum TYPE {L1, L2}
    private final int id;

    //DEBUG ONLY: assumption that the cache is always up
    private boolean crashed = false;
    private final TYPE type_of_cache;
    private Map<Integer, Integer> data = new HashMap<>();

    // since we use the same class for both types of cache
    // we don't distinguish between different types of children
    // (clients for L2 cache, L2 cache for L1 cache), same for the parent
    private Set<ActorRef> children = new HashSet<>();

    private ActorRef parent;
    private final ActorRef database;

    private Random rnd = new Random();
    private String classString = String.valueOf(getClass());

    public Cache(int id, String type, ActorRef parent) {
        this.id = id;
        this.parent = parent;

        if (type.equals("L1")){
            this.type_of_cache = TYPE.L1;
            this.database = parent;
        } else if (type.equals("L2")) {
            throw new IllegalArgumentException("Database address not specified!");
        } else {
            throw new IllegalArgumentException("Wrong type of cache requested!");
        }

    }

    public Cache(int id, String type, ActorRef parent, ActorRef database) {
        this.id = id;

        if (type.equals("L1")){
            this.type_of_cache = TYPE.L1;
        } else if (type.equals("L2")) {
            this.type_of_cache = TYPE.L2;
        } else {
            throw new IllegalArgumentException("Wrong type of cache requested!");
        }

        this.parent = parent;
        this.database = database;
    }

    static public Props props(int id, String type, ActorRef parent) {
        return Props.create(Cache.class, () -> new Cache(id, type, parent));
    }

    static public Props props(int id, String type, ActorRef parent, ActorRef database) {
        return Props.create(Cache.class, () -> new Cache(id, type, parent, database));
    }

    /*-- Actor logic -- */
    public void preStart() {

        CustomPrint.print(classString,"Cache " + id + " started");
    }


    // Here we define the mapping between the received message types and the database methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchAny(o -> System.out.println("Cache " + id +" received unknown message from " + getSender()))
                .build();
    }
}
