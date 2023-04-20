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

    private int getId(){
        return this.id;
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

    public Integer getTimeout(String timeout){
        return this.timeouts.get(timeout);
    public Integer getTimeout(String timeout){
        return this.timeouts.get(timeout);
    }

    private void startTimeout(String type){
        getContext().system().scheduler().scheduleOnce(
                Duration.create(getTimeout(type), TimeUnit.SECONDS),
                getSelf(),
                new TimeoutMsg(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    private boolean hasResponded(){return this.responded;}

    private void sendRequest(){ this.responded = false;}
    private void receivedResponse(){ this.responded = true;}

    /*-- Actor logic -- */

    public void preStart() {}

    // ----------SEND LOGIC----------
    private void sendInitMsg(){
        InitMsg msg = new InitMsg(getSelf(), "client");
        getParent().tell(msg, getSelf());
    }

    private void sendWriteRequestMsg(int key, int value) {
    private void sendWriteRequestMsg(int key, int value) {
        Stack<ActorRef> path = new Stack<>();
        WriteRequestMsg msg = new WriteRequestMsg(key, value, path, getSelf());
        getParent().tell(msg, getSelf());
        sendRequest();
        getContext().system().scheduler().scheduleOnce(
                Duration.create(getTimeout("write"), TimeUnit.SECONDS),
                getSelf(),
                new TimeoutMsg(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    private void sendCriticalReadRequestMsg(int key){
        Stack<ActorRef> path = new Stack<>();
        path.push(getSelf());
        WriteRequestMsg msg = new WriteRequestMsg(key, value, path);
        getParent().tell(msg, getSelf());
        CriticalReadRequestMsg msg = new CriticalReadRequestMsg(key, path, getSelf());
        getParent().tell(msg, getSelf());
        sendRequest();
        startTimeout("write");
    }

    private void sendCriticalReadRequestMsg(int key){
        Stack<ActorRef> path = new Stack<>();
        path.push(getSelf());
        CriticalReadRequestMsg msg = new CriticalReadRequestMsg(key, path);
        getParent().tell(msg, getSelf());
        sendRequest();
        startTimeout("crit_read");
        getContext().system().scheduler().scheduleOnce(
                Duration.create(getTimeout("crit_read"), TimeUnit.SECONDS),
                getSelf(),
                new TimeoutMsg(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }


    private void sendCriticalWriteMsg(int key, int value) {
        Stack<ActorRef> path = new Stack<>();
        path.push(getSelf());
        CriticalWriteRequestMsg msg = new CriticalWriteRequestMsg(key, value, path);
        getParent().tell(msg, getSelf());
        sendRequest();
        startTimeout("crit_write");
    }

    // ----------RECEIVE LOGIC----------

    // Here we define the mapping between the received message types and the database methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartInitMsg.class, this::onStartInitMsg)
                .match(StartWriteMsg.class, this::onStartWriteMsg)
                .match(StartCriticalReadMsg.class, this::onStartCriticalReadMsg)
                .match(StartCriticalWriteMsg.class, this::onStartCriticalWriteMsg)
                .match(WriteResponseMsg.class, this::onWriteResponseMsg)
                .match(CriticalReadResponseMsg.class, this::onCriticalReadResponseMsg)
                .match(StartCriticalReadMsg.class, this::onStartCriticalReadMsg)
                .match(WriteResponseMsg.class, this::onWriteResponseMsg)
                .match(CriticalReadResponseMsg.class, this::onCriticalReadResponseMsg)
                .match(TimeoutMsg.class, this::onTimeoutMsg)
                .match(TimeoutElapsedMsg.class, this::onTimeoutElapsedMsg)
                .match(InfoMsg.class, this::onInfoMsg)
                .matchAny(o -> log.debug("[CLIENT " + id + "] received unknown message from " +
                        getSender().path().name() + ": " + o))
                .build();
    }

    private void onInfoMsg(InfoMsg msg){
        log.info("[CLIENT {}] Parent: ", getId(), getParent().path().name());
    }

    private void onStartInitMsg(StartInitMsg msg) {
        // log.info("[CLIENT " + id + "] Received initialization msg!");
        sendInitMsg();
    }

    private void onTimeoutMsg(TimeoutMsg msg) {
        if (!hasResponded()) {
            log.info("[CLIENT " + id + "] Received timeout msg from {}!", getSender().path().name());
            receivedResponse();
            log.info("[CLIENT " + id + "] Connecting to another L2 cache");
            Set<ActorRef> caches = getL2_caches();
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
            setParent(cache);
            getParent().tell(new RequestConnectionMsg(), getSelf());
        }
    }

    private void onTimeoutElapsedMsg(TimeoutElapsedMsg msg){
        log.info("[CLIENT " + id + "] Received timeout msg from {}!", getSender().path().name());
        receivedResponse();
    }

    // ----------WRITE MESSAGES LOGIC----------
    private void onStartWriteMsg(StartWriteMsg msg) {
        log.info("[CLIENT " + id + "] Received write msg!");
        sendWriteRequestMsg(msg.getKey(), msg.getValue());
        sendWriteRequestMsg(msg.getKey(), msg.getValue());
    }

    private void onWriteResponseMsg(WriteResponseMsg msg) {
        if (!hasResponded()) {
            receivedResponse();
            log.info("[CLIENT {}] Successful write operation of value {} for key {}",
                    this.id, msg.getValue(), msg.getKey());
        }
    }

    private void onStartCriticalWriteMsg(StartCriticalWriteMsg msg){
        log.info("[CLIENT " + id + "] Received write msg!");
        sendCriticalWriteMsg(msg.getKey(), msg.getValue());
    }

    // ----------READ MESSAGES LOGIC----------
    private void onStartCriticalReadMsg(StartCriticalReadMsg msg){
        sendCriticalReadRequestMsg(msg.getKey());
    }

    private void onWriteResponseMsg(WriteResponseMsg msg) {
        if (!hasResponded()) {
    private void onCriticalReadResponseMsg(CriticalReadResponseMsg msg){
        if (!hasResponded()){
            receivedResponse();
            log.info("[CLIENT {}] Successful write operation of value {} for key {}",
                    this.id, msg.getValue(), msg.getKey());
        }
    }

    // ----------READ MESSAGES LOGIC----------
    private void onStartCriticalReadMsg(StartCriticalReadMsg msg){
        sendCriticalReadRequestMsg(msg.getKey());
    }

    private void onCriticalReadResponseMsg(CriticalReadResponseMsg msg){
        if (!hasResponded()){
            receivedResponse();
            log.info("[CLIENT {}][CRITICAL] Received response from read containing value {} for key {}",
                    this.id, msg.getValue(), msg.getKey());
            log.info("[CLIENT {}][CRITICAL] Received response from read containing value {} for key {}",
                    this.id, msg.getValue(), msg.getKey());
        }
    }
}
