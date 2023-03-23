package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.HashSet;
import java.util.Set;

import java.util.Random;

public class Client extends AbstractActor {

    private int id;

    //DEBUG ONLY: assumption that clients are always up
    private boolean crashed = false;

    private Set<ActorRef> L2_caches = new HashSet<>();

    private Random rnd = new Random();
    private String classString = String.valueOf(getClass());

    public Client(int id) {
        this.id = id;
    }

    static public Props props(int id) {
        return Props.create(Client.class, () -> new Client(id));
    }


    /*-- Actor logic -- */

    @Override
    public void preStart() {

        CustomPrint.print(classString,"Client " + id + " started");
    }


    // Here we define the mapping between the received message types and the database methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .matchAny(o -> System.out.println("Client " + id +" received unknown message from " + getSender()))
            .build();
    }

}
