package it.unitn.ds1;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static akka.pattern.Patterns.ask;
import static akka.pattern.Patterns.pipe;
import static java.lang.Thread.sleep;

public class Cache extends AbstractActor{

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private enum TYPE {L1, L2}
    private final int id;
    //DEBUG ONLY: assumption that the cache is always up
    private boolean crashed = false;
    private boolean responded = false;
    private final TYPE type_of_cache;

    private Map<Integer, Integer> data = new HashMap<>();

    // since we use the same class for both types of cache
    // we don't distinguish between different types of children
    // (clients for L2 cache, L2 cache for L1 cache), same for the parent
    private Set<ActorRef> children = new HashSet<>();

    private ActorRef parent;

    private final ActorRef database;

    private final HashMap<String, Integer> timeouts = new HashMap<>();

    private Random rnd = new Random();

    private String classString = String.valueOf(getClass());

    // ----------INITIALIZATION LOGIC----------
    public Cache(int id,
                 String type,
                 ActorRef parent,
                 List<TimeoutConfiguration> timeouts) throws IOException {

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
        setTimeouts(timeouts);

        // log.info("[{} CACHE {}] Cache initialized!", this.type_of_cache.toString(), String.valueOf(this.id));
    }

    public Cache(int id,
                 String type,
                 ActorRef parent,
                 ActorRef database,
                 List<TimeoutConfiguration> timeouts) throws IOException {

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
        setTimeouts(timeouts);

        // log.info("[{} CACHE {}] Cache initialized!", this.type_of_cache.toString(), String.valueOf(this.id));
    }

    static public Props props(int id, String type, ActorRef parent, List<TimeoutConfiguration> timeouts) {
        return Props.create(Cache.class, () -> new Cache(id, type, parent, timeouts));
    }

    static public Props props(int id, String type, ActorRef parent, ActorRef database, List<TimeoutConfiguration> timeouts) {
        return Props.create(Cache.class, () -> new Cache(id, type, parent, database, timeouts));
    }

    // ----------CRASHING LOGIC----------

    public void crash() {
        this.crashed = true;
        clearData();
        log.info("[{} CACHE {}] Cache crashed!", this.type_of_cache.toString(), String.valueOf(this.id));
    }

    public boolean isCrashed(){
        return this.crashed;
    }

    public void recover() {
        this.crashed = false;
        log.info("[{} CACHE {}] Recovery process started!",
                this.type_of_cache.toString(), String.valueOf(this.id));
    }

    //----------DATA LOGIC----------

    public void addData(int key, int value) {
        this.data.put(key, value);
    }

    public int getData(int key) {
        return this.data.get(key);
    }

    public boolean isDataPresent(int key) {
        return this.data.containsKey(key);
    }

    public void clearData() {
        this.data.clear();
    }

    //----------CHILDREN LOGIC----------

    public void addChild(ActorRef child){
        this.children.add(child);
    }

    public void removeChild(ActorRef child){
        this.children.remove(child);
    }

    public Set<ActorRef> getChildren(){
        return this.children;
    }

    public void setChildren(Set<ActorRef> children) {
        this.children = children;
    }

    public void setChildren(List<ActorRef> children) {
        for (ActorRef child : children){
            addChild(child);
        }
    }

    // ----------PARENT LOGIC----------

    public void setParent(ActorRef parent) {
        this.parent = parent;
    }

    public ActorRef getParent(){
        return this.parent;
    }

    // ----------DATABASE LOGIC----------

    public ActorRef getDatabase(){
        return this.database;
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

    public boolean hasResponded(){ return this.responded;}

    public void sendRequest(){this.responded = false;}

    public void receivedResponse(){this.responded = true;}

    private void waitSomeTime(int time){

        long t0 = System.currentTimeMillis();
        long timeSpent = 0;
        while (timeSpent <= time*1000L) {
            timeSpent = System.currentTimeMillis() - t0;
        }
    }

    /*-- Actor logic -- */

    public void preStart() {
    }

    // ----------SEND LOGIC----------

    private void onStartInitMsg(Message.StartInitMsg msg){
        sendInitMsg();
    }

    private void sendInitMsg(){
        Message.InitMsg msg = new Message.InitMsg(getSelf(), this.type_of_cache.toString());
        parent.tell(msg, getSelf());

    }

    // ----------RECEIVE LOGIC----------

    // Here we define the mapping between the received message types and the database methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.StartInitMsg.class, this::onStartInitMsg)
                .match(Message.InitMsg.class, this::onInitMsg)
                .match(Message.WriteMsg.class, this::onWriteMsg)
                .match(Message.WriteConfirmationMsg.class, this::onWriteConfirmationMsg)
                .match(Message.FillMsg.class, this::onFillMsg)
                .match(Message.CrashMsg.class, this::onCrashMsg)
                .match(Message.RecoverMsg.class, this::onRecoverMsg)
                .match(Message.TimeoutMsg.class, this::onTimeoutMsg)
                .matchAny(o -> log.info("[{} CACHE {}] Received unknown message from {} {}!",
                        this.type_of_cache.toString(), String.valueOf(this.id), getSender().path().name(), o.getClass().getName()))
                .build();
    }

    // ----------TIMEOUT MESSAGE LOGIC----------
    private void onTimeoutMsg(Message.TimeoutMsg msg) {
        if (!hasResponded()) {
            log.info("[{} CACHE {}] Received timeout msg from {}!",
                    this.type_of_cache, this.id, getSender().path().name());
            receivedResponse();
        }
    }

    // ----------WRITE MESSAGES LOGIC----------
    private void onWriteConfirmationMsg(Message.WriteConfirmationMsg msg){
        if (!isCrashed()) {
            if (!hasResponded()) {
                waitSomeTime(15);

                ActorRef destination = msg.path.pop();
                destination.tell(msg, getSelf());

                if (type_of_cache == TYPE.L1) {
                    for (ActorRef child : getChildren()) {
                        if (!child.equals(destination)) {
                            child.tell(new Message.FillMsg(msg.key, msg.value), getSelf());
                        }
                    }
                }

                if (data.containsKey(msg.key)) {
                    data.put(msg.key, msg.value);
                }

                log.info("[{} CACHE {}] Received write confirmation!",
                        this.type_of_cache.toString(), String.valueOf(this.id));

                receivedResponse();
            }
        }
    }

    private void onWriteMsg(Message.WriteMsg msg){
        if(!isCrashed()) {
            msg.path.push(getSelf());
            parent.tell(msg, getSelf());
            sendRequest();
            getContext().getSystem().getScheduler().scheduleOnce(
                    Duration.create(timeouts.get("write")*1000, TimeUnit.MILLISECONDS),
                    getSelf(),
                    new Message.TimeoutMsg(), // the message to send
                    getContext().system().dispatcher(),
                    getSelf()
            );
        }
    }

    private void onFillMsg(Message.FillMsg msg){
        if(!isCrashed()){
            if (this.data.containsKey(msg.key)){
                this.data.put(msg.key, msg.value);
            }
            for (ActorRef child : this.children){
                Message.FillMsg fillMsg = new Message.FillMsg(msg.key, msg.value);
                if (child.path().name().contains("cache")){
                    child.tell(fillMsg, getSelf());
                    // log.info("[{} CACHE {}] Sent fill msg to {}!",
                    //         this.type_of_cache.toString(), String.valueOf(this.id), child.path().name());
                }
            }
        }
    }

    // ----------INITIALIZATION MESSAGES LOGIC----------
    private void onInitMsg(Message.InitMsg msg) throws InvalidMessageException{
        if ((this.type_of_cache == TYPE.L1 && !Objects.equals(msg.type, "L2")) ||
                (this.type_of_cache == TYPE.L2 && !Objects.equals(msg.type, "client"))){
            throw new InvalidMessageException("Message to wrong destination!");
        }

        addChild(msg.id);
        String log_msg = " Added " + getSender().path().name() + " as a child";
        // log.info("[{} CACHE {}] " + log_msg, this.type_of_cache.toString(), String.valueOf(this.id));
    }

    // ----------CRASH MESSAGES LOGIC----------
    private void onCrashMsg(Message.CrashMsg msg){
        if (!isCrashed()){
            crash();
        }
    }

    HashSet<Integer> recoveredKeys = new HashSet<>();
    HashMap<ActorRef, HashMap<Integer, Integer>> recoveredValuesForChild = new HashMap<>();
    HashSet<ActorRef> childrenToRecover = new HashSet<>();

    private void onRecoverMsg(Message.RecoverMsg msg){
        if (isCrashed()){
            recover();
            // L2 caches does not need to be repopulated with data from before crash
            // they will repopulate with data coming from new reads
            if (this.type_of_cache == TYPE.L1){
                // todo: ask for data for all children
                for (ActorRef child : getChildren()){
                    child.tell("", getSelf());
                }
                // todo: ask data to parent/database (read/crit read)
                // todo: populate data
            }

        }
    }
}
