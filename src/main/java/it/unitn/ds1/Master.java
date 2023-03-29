package it.unitn.ds1;

import akka.actor.ActorRef;

import java.util.HashSet;

public class Master {

    private HashSet<ActorRef> l1CacheActors;
    private HashSet<ActorRef> l2CacheActors;
    private HashSet<ActorRef> clientActors;

    public Master(HashSet<ActorRef> l1CacheActors,
                  HashSet<ActorRef> l2CacheActors,
                  HashSet<ActorRef> clientActors) {
        setL1CacheActors(l1CacheActors);
        setL2CacheActors(l2CacheActors);
        setClientActors(clientActors);
    }

    // ----------SYSTEM SETUP----------

    public HashSet<ActorRef> getL1CacheActors() {
        return l1CacheActors;
    }

    public void setL1CacheActors(HashSet<ActorRef> l1CacheActors) {
        this.l1CacheActors = l1CacheActors;
    }

    public HashSet<ActorRef> getL2CacheActors() {
        return l2CacheActors;
    }

    public void setL2CacheActors(HashSet<ActorRef> l2CacheActors) {
        this.l2CacheActors = l2CacheActors;
    }

    public HashSet<ActorRef> getClientActors() {
        return clientActors;
    }

    public void setClientActors(HashSet<ActorRef> clientActors) {
        this.clientActors = clientActors;
    }
}

