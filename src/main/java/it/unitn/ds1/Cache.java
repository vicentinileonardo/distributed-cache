package it.unitn.ds1;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

import it.unitn.ds1.Message.*;

public class Cache extends AbstractActor{

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private enum TYPE {L1, L2}
    private final int id;
    //DEBUG ONLY: assumption that the cache is always up
    private boolean crashed = false;

    private boolean responded = false;
    private ActorRef sender = null;
    private Map.Entry<ActorRef, Boolean> response = new AbstractMap.SimpleEntry<>(null, true);
    private final TYPE type_of_cache;

    private Map<Integer, Integer> data = new HashMap<>();

    HashSet<Integer> recoveredKeys = new HashSet<>();
    HashMap<ActorRef, Map<Integer, Integer>> recoveredValuesForChild = new HashMap<>();
    HashSet<ActorRef> childrenToRecover = new HashSet<>();
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
                 List<TimeoutConfiguration> timeouts) {

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
                 List<TimeoutConfiguration> timeouts) {

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

    private boolean hasChildren(){
        return !this.children.isEmpty();
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
            setTimeout(timeout.getType(), timeout.getValue());
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

    private void startTimeout(String type){
        getContext().getSystem().getScheduler().scheduleOnce(
                Duration.create(timeouts.get(type)*1000, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutMsg(), // the message to send
                getContext().system().dispatcher(),
                getSelf()
        );
    }

    public boolean hasResponded(){ return this.response.getValue();}

    public void sendRequest(ActorRef sender){
        this.response = new AbstractMap.SimpleEntry<>(sender, false);
    }

    public void receivedResponse(){
        this.response = new AbstractMap.SimpleEntry<>(null, true);
    }

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

    private void onStartInitMsg(StartInitMsg msg){
        sendInitMsg();
    }

    private void sendInitMsg(){
        InitMsg msg = new InitMsg(getSelf(), this.type_of_cache.toString());
        parent.tell(msg, getSelf());

    }

    // ----------RECEIVE LOGIC----------

    // Here we define the mapping between the received message types and the database methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartInitMsg.class, this::onStartInitMsg)
                .match(InitMsg.class, this::onInitMsg)
                .match(WriteRequestMsg.class, this::onWriteMsg)
                .match(WriteResponseMsg.class, this::onWriteConfirmationMsg)
//                .match(CriticalWriteRequestMsg.class, this::onCriticalWriteRequestMsg)
//                .match(CriticalWriteResponseMsg.class, this::onCriticalWriteResponseMsg)
                .match(FillMsg.class, this::onFillMsg)
                .match(RefillMsg.class, this::onRefillMsg)
                .match(CrashMsg.class, this::onCrashMsg)
                .match(RecoverMsg.class, this::onRecoverMsg)
                .match(TimeoutMsg.class, this::onTimeoutMsg)
                .match(TimeoutElapsedMsg.class, this::onTimeoutElapsedMsg)
                .match(ResponseDataRecoverMsg.class, this::onResponseDataRecoverMsg)
                .match(RequestDataRecoverMsg.class, this::onRequestDataRecoverMsg)
                .match(ResponseUpdatedDataMsg.class, this::onResponseUpdatedDataMsg)
                .match(UpdateDataMsg.class, this::onUpdateDataMsg)
                .match(RequestConnectionMsg.class, this::onRequestConnectionMsg)
                .matchAny(o -> log.info("[{} CACHE {}] Received unknown message from {} {}!",
                        this.type_of_cache.toString(), String.valueOf(this.id), getSender().path().name(), o.getClass().getName()))
                .build();
    }

    // ----------TIMEOUT MESSAGE LOGIC----------
    private void onTimeoutMsg(TimeoutMsg msg) {
        if (!hasResponded()) {
            log.info("[{} CACHE {}] Received timeout msg from {}!",
                    this.type_of_cache, this.id, getSender().path().name());
            receivedResponse();
            log.info("[{} CACHE {}] Connecting to DATABASE",
                    this.type_of_cache.toString(), String.valueOf(this.id));
            this.parent = this.database;
            this.parent.tell(new RequestConnectionMsg("L2"), getSelf());
            this.response.getKey().tell(new TimeoutElapsedMsg(), getSelf());
        }
    }

    private void onRequestConnectionMsg(RequestConnectionMsg msg){
        addChild(getSender());
    }

    private void onTimeoutElapsedMsg(TimeoutElapsedMsg msg){

    }

    // ----------WRITE MESSAGES LOGIC----------
    private void onWriteConfirmationMsg(WriteResponseMsg msg){
        if (!isCrashed()) {
            if (!hasResponded()) {
                Stack<ActorRef> tmpStack = msg.getPath();
                ActorRef destination = tmpStack.pop();
                destination.tell(new WriteResponseMsg(msg.getKey(), msg.getValue(), tmpStack), getSelf());

                if (type_of_cache == TYPE.L1) {
                    for (ActorRef child : getChildren()) {
                        if (!child.equals(destination)) {
                            child.tell(new FillMsg(msg.getKey(), msg.getValue()), getSelf());
                        }
                    }
                }

                if (isDataPresent(msg.getKey())) {
                    addData(msg.getKey(), msg.getValue());
                }

                log.info("[{} CACHE {}] Received write confirmation!",
                        this.type_of_cache.toString(), String.valueOf(this.id));

                receivedResponse();
            }
        }
    }

    private void onWriteMsg(WriteRequestMsg msg){
        if(!isCrashed()) {
            Stack<ActorRef> tmpStack = msg.getPath();
            tmpStack.push(getSelf());
            parent.tell(new WriteRequestMsg(msg.getKey(), msg.getValue(), tmpStack), getSelf());
            sendRequest(getSender());
            startTimeout("write");
        }
    }

    private void onFillMsg(FillMsg msg){
        if(!isCrashed()){
            if (isDataPresent(msg.getKey())){
                addData(msg.getKey(), msg.getValue());
            }
            if (hasChildren()) {
                for (ActorRef child : this.children) {
                    if (child.path().name().contains("cache")) {
                        FillMsg fillMsg = new FillMsg(msg.getKey(), msg.getValue());
                        child.tell(fillMsg, getSelf());
                    }
                }
            }
        }
    }
//
//    private void onCriticalWriteRequestMsg(CriticalWriteRequestMsg msg){
//        if (!isCrashed()){
//            Stack<ActorRef> tmpStack = msg.getPath();
//            tmpStack.push(getSelf());
//            parent.tell(new CriticalWriteRequestMsg(msg.getKey(), msg.getValue(), tmpStack), getSelf());
//            sendRequest(getSender());
//            startTimeout("crit_write");
//        }
//    }
//
//    private void onCriticalWriteResponseMsg(CriticalWriteResponseMsg msg){
//        if(!isCrashed()){
//            if(!hasResponded()){
//                receivedResponse();
//                Stack<ActorRef> tmpStack = msg.getPath();
//                ActorRef destination = tmpStack.pop();
//                destination.tell(new CriticalWriteResponseMsg(msg.getKey(), msg.getValue(), tmpStack), getSelf());
//
//                if (isDataPresent(msg.getKey())){
//                    addData(msg.getKey(), msg.getValue());
//                }
//
//                log.info("[{} CACHE {}][CRITICAL] Received write confirmation!",
//                        this.type_of_cache.toString(), String.valueOf(this.id));
//
//            }
//        }
//    }

    private void onRefillMsg(RefillMsg msg) {
        if (!isCrashed()){
            if (isDataPresent(msg.getKey())){
                addData(msg.getKey(), msg.getValue());
            }
            if (hasChildren()) {
                for (ActorRef child : getChildren()) {
                    if (!child.path().name().contains("client")) {
                        child.tell(new RefillMsg(msg.getKey(), msg.getValue()), getSelf());
                        // TODO: add list of child to wait
                        startTimeout("crit_write");
                    }
                }
            }
        }
    }

    // ----------INITIALIZATION MESSAGES LOGIC----------
    private void onInitMsg(InitMsg msg) throws InvalidMessageException{
        if ((this.type_of_cache == TYPE.L1 && !Objects.equals(msg.getType(), "L2")) ||
                (this.type_of_cache == TYPE.L2 && !Objects.equals(msg.getType(), "client"))){
            throw new InvalidMessageException("Message to wrong destination!");
        }

        addChild(msg.getId());
    }

    // ----------CRASH MESSAGES LOGIC----------
    private void onCrashMsg(CrashMsg msg){
        if (!isCrashed()){
            crash();
        }
    }

    private void onRecoverMsg(RecoverMsg msg){
        if (isCrashed()){
            recover();
            // L2 caches does not need to be repopulated with data from before crash
            // they will repopulate with data coming from new reads
            if (this.type_of_cache == TYPE.L1){
                if (hasChildren()) {
                    for (ActorRef child : getChildren()) {
                        child.tell(new RequestDataRecoverMsg(), getSelf());
                        childrenToRecover.add(child);
                    }
                }
            }
        }
    }

    private void onResponseDataRecoverMsg(ResponseDataRecoverMsg msg){
        childrenToRecover.remove(getSender());
        if (msg.getParent() != getSelf()){
            children.remove(getSender());
        }

        recoveredKeys.addAll(msg.getData().keySet());
        recoveredValuesForChild.put(getSender(), msg.getData());

        if (childrenToRecover.isEmpty()){
            parent.tell(new RequestUpdatedDataMsg(recoveredKeys), getSelf());
        }
    }

    private void onRequestDataRecoverMsg(RequestDataRecoverMsg msg){
        getSender().tell(new ResponseDataRecoverMsg(this.data, this.parent), getSelf());
    }

    private void onResponseUpdatedDataMsg(ResponseUpdatedDataMsg msg){
        this.data = msg.getData();

        for (Map.Entry<ActorRef , Map<Integer, Integer>> entry: this.recoveredValuesForChild.entrySet()){
            ActorRef child = entry.getKey();
            Map<Integer, Integer> tmpData = new HashMap<>();
            for (Map.Entry<Integer, Integer> dataEntry: entry.getValue().entrySet()){
                int key = dataEntry.getKey();
                int value = dataEntry.getValue();
                if (msg.getData().get(key) != value){
                    tmpData.put(key, value);
                }
            }

            if (!tmpData.isEmpty()) {
                child.tell(new UpdateDataMsg(tmpData), getSelf());
            }
        }
    }

    private void onUpdateDataMsg(UpdateDataMsg msg){
        this.data.putAll(msg.getData());
    }
}
