package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static akka.pattern.Patterns.*;

public class Client extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
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

    public void setParent(ActorRef parent) {
        this.parent = parent;
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

    public void preStart() {
        // CustomPrint.infoPrint(classString,"", String.valueOf(this.id), " Started!");
        log.info("[CLIENT " + id + "] Started!");
    }

    // ----------SEND LOGIC----------
    public void sendInitMsg(){
        Message.InitMsg msg = new Message.InitMsg(getSelf(), "client");
        parent.tell(msg, getSelf());
    }

    public void sendWriteMsg(int key, int value){
        Stack<ActorRef> path = new Stack<>();
        path.push(getSelf());
        Message.WriteMsg msg = new Message.WriteMsg(key, value, path);
        parent.tell(msg, getSelf());
        // CustomPrint.infoPrint(classString,"", String.valueOf(this.id), " Sent write msg!");
        log.debug("[CLIENT " + id + "] Sent write msg!");
    }

    // ----------RECEIVE LOGIC----------

    // Here we define the mapping between the received message types and the database methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.StartInitMsg.class, this::onStartInitMsg)
                .match(Message.StartWriteMsg.class, this::onStartWriteMsg)
                .match(Message.WriteConfirmationMsg.class, this::onWriteConfirmationMsg)
                .matchAny(o -> log.info("[CLIENT " + id + "] received unknown message from " +
                        getSender().path().name()))
                .build();
    }

    private void onStartInitMsg(Message.StartInitMsg msg) {
        // CustomPrint.infoPrint(classString,"", String.valueOf(this.id), " Received initialization msg!");
        log.info("[CLIENT " + id + "] Received initialization msg!");
        sendInitMsg();
    }

    // ----------WRITE MESSAGES LOGIC----------
    private void onStartWriteMsg(Message.StartWriteMsg msg) {
        // CustomPrint.infoPrint(classString,"", String.valueOf(this.id), " Received write msg!");
        log.info("[CLIENT " + id + "] Received write msg!");
        sendWriteMsg(msg.key, msg.value);
    }

    private void onWriteConfirmationMsg(Message.WriteConfirmationMsg msg) {
        // CustomPrint.infoPrint(classString,"", String.valueOf(this.id), " Received write confirmation msg!");
        log.info("[CLIENT " + id + "] Received write confirmation msg!");    }
}
