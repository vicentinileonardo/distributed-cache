package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

import it.unitn.ds1.Message.*;

public class Client extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private int id;

    //DEBUG ONLY: assumption that clients are always up
    private boolean crashed = false;
    private boolean responded = true;

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

    public boolean hasResponded(){return this.responded;}

    public void sendRequest(){ this.responded = false;}
    public void receivedResponse(){ this.responded = true;}

    /*-- Actor logic -- */

    public void preStart() {}

    // ----------SEND LOGIC----------
    public void sendInitMsg(){
        InitMsg msg = new InitMsg(getSelf(), "client");
        parent.tell(msg, getSelf());
    }

    public void sendWriteMsg(int key, int value) {
        Stack<ActorRef> path = new Stack<>();
        path.push(getSelf());
        WriteMsg msg = new WriteMsg(key, value, path);
        parent.tell(msg, getSelf());
        sendRequest();
        getContext().system().scheduler().scheduleOnce(
                Duration.create(timeouts.get("write"), TimeUnit.SECONDS),
                getSelf(),
                new TimeoutMsg(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }
    // ----------RECEIVE LOGIC----------

    // Here we define the mapping between the received message types and the database methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartInitMsg.class, this::onStartInitMsg)
                .match(StartWriteMsg.class, this::onStartWriteMsg)
                .match(TimeoutMsg.class, this::onTimeoutMsg)
                .match(WriteConfirmationMsg.class, this::onWriteConfirmationMsg)
                .match(TimeoutElapsedMsg.class, this::onTimeoutElapsedMsg)
                .matchAny(o -> log.info("[CLIENT " + id + "] received unknown message from " +
                        getSender().path().name() + ": " + o))
                .build();
    }

    private void onStartInitMsg(StartInitMsg msg) {
        // log.info("[CLIENT " + id + "] Received initialization msg!");
        sendInitMsg();
    }

    private void onTimeoutMsg(TimeoutMsg msg) {
        if (hasResponded()) {
            log.info("[CLIENT " + id + "] Received timeout msg from {}!", getSender().path().name());
            receivedResponse();
            log.info("[CLIENT " + id + "] Connecting to another L2 cache");
            Set<ActorRef> caches = this.getL2_caches();
            ActorRef[] tmpArray = caches.toArray(new ActorRef[caches.size()]);
            ActorRef cache = null;
            while(cache == this.parent || cache == null) {
                // generate a random number
                Random rnd = new Random();

                // this will generate a random number between 0 and
                // HashSet.size - 1
                int rndNumber = rnd.nextInt(caches.size());
                cache = tmpArray[rndNumber];
            }
            this.parent = cache;
            parent.tell(new RequestConnectionMsg(), getSelf());
        }
    }

    private void onTimeoutElapsedMsg(TimeoutElapsedMsg msg){
        log.info("[CLIENT " + id + "] Received timeout msg from {}!", getSender().path().name());
        receivedResponse();
    }

    // ----------WRITE MESSAGES LOGIC----------
    private void onStartWriteMsg(StartWriteMsg msg) {
        log.info("[CLIENT " + id + "] Received write msg!");
        sendWriteMsg(msg.key, msg.value);
    }

    private void onWriteConfirmationMsg(WriteConfirmationMsg msg) {
        if (hasResponded()) {
            receivedResponse();
            log.info("[CLIENT {}] Received Message containing key {} and value {}", id, msg.key, msg.value);
        }
    }
}
