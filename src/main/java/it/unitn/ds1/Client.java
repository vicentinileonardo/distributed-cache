package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.HashMap;

import java.util.HashSet;
import java.util.List;
import java.util.Random;

public class Client extends AbstractActor {

    private int id;

    //DEBUG ONLY: assumption that clients are always up
    private boolean crashed = false;

    private HashSet<ActorRef> L2_caches = new HashSet<>();

    private ActorRef parent;

    private HashMap<String, Integer> timeouts = new HashMap<>();

    private Random rnd = new Random();
    private String classString = String.valueOf(getClass());

    public Client(int id, ActorRef parent, List<TimeoutConfiguration> timeouts, HashSet<ActorRef> l2Caches) {
        this.id = id;
        setParent(parent);
        setTimeouts(timeouts);
        setL2_caches(l2Caches);
    }

    static public Props props(int id, ActorRef parent, List<TimeoutConfiguration> timeouts, HashSet<ActorRef> l2Caches) {
        return Props.create(Client.class, () -> new Client(id, parent, timeouts, l2Caches));
    }

    // ----------PARENT LOGIC----------

    public ActorRef getParent() {
        return this.parent;
    }

    public void setParent(ActorRef l2_cache) {
        this.parent = l2_cache;
    }

    // ----------L2 CACHE LOGIC----------

    public HashSet<ActorRef> getL2_caches() {
        return this.L2_caches;
    }

    public void setL2_caches(HashSet<ActorRef> l2_caches) {
        this.L2_caches = l2_caches;
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
        CustomPrint.print(classString,"", String.valueOf(this.id), " Started!");
    }

    // ----------SEND LOGIC----------
    public void sendInitMsg(){
        Message.InitMsg msg = new Message.InitMsg(getSelf(), "client");
        parent.tell(msg, getSelf());
    }

    // ----------RECEIVE LOGIC----------

    // Here we define the mapping between the received message types and the database methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.StartInitMsg.class, this::onStartInitMsg)
                .matchAny(o -> System.out.println("Client " + id +" received unknown message from " + getSender()))
                .build();
    }

    private void onStartInitMsg(Message.StartInitMsg msg) {
        sendInitMsg();
    }
}
