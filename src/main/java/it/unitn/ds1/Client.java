package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.HashMap;

import java.util.List;
import java.util.Random;

public class Client extends AbstractActor {

    private int id;

    //DEBUG ONLY: assumption that clients are always up
    private boolean crashed = false;

    private ActorRef L2_cache;

    private HashMap<String, Integer> timeouts = new HashMap<>();

    private Random rnd = new Random();
    private String classString = String.valueOf(getClass());

    public Client(int id, ActorRef parent, List<TimeoutConfiguration> timeouts) {
        this.id = id;
        setL2_cache(parent);
        setTimeouts(timeouts);
    }

    static public Props props(int id, ActorRef parent, List<TimeoutConfiguration> timeouts) {
        return Props.create(Client.class, () -> new Client(id, parent, timeouts));
    }

    public ActorRef getL2_cache() {
        return this.L2_cache;
    }

    public void setL2_cache(ActorRef l2_cache) {
        this.L2_cache = l2_cache;
    }

    // ----------TIMEOUT LOGIC----------

    public void setTimeouts(List<TimeoutConfiguration> timeouts){
        for (TimeoutConfiguration timeout: timeouts){
            this.timeouts.put(timeout.getType(), timeout.getValue());
        }
    }

    public HashMap<String, Integer> getTimeouts(){
        return this.timeouts;
    }

    public int getTimeout(String type){
        return this.timeouts.get(type);
    }

    public void setTimeout(String type, int value){
        this.timeouts.put(type, value);
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
