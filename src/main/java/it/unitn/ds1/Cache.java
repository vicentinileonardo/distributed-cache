package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.InvalidMessageException;
import akka.actor.Props;
import scala.collection.View;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

import java.io.IOException;
import java.util.*;

import akka.event.Logging;
import akka.event.LoggingAdapter;

import it.unitn.ds1.Message.*;

public class Cache extends AbstractActor{

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private enum TYPE {L1, L2}

    private final int id;

    private boolean crashed = false;

    private final TYPE type_of_cache;

    private Map<Integer, Integer> data = new HashMap<>();

    public class Request {

        private final long requestId;
        private final int key;
        private int value;
        private final String type;
        private final String requesterType;
        private ActorRef requester; //either a client or a cache???
        private boolean isFulfilled = false;

        public Request(String type, String requesterType, ActorRef requester, int key, long requestId) {
            this.type = type;
            this.requesterType = requesterType;
            this.requester = requester;
            this.key = key;
            this.requestId = requestId;
        }

        long getRequestId() {
            return requestId;
        }

        int getKey() {
            return key;
        }

        int getValue() {
            return value;
        }

        String getType() {
            return type;
        }

        String getRequesterType() {
            return requesterType;
        }

        ActorRef getRequester() {
            return requester;
        }

        public boolean isFulfilled() {
            return isFulfilled;
        }

        void setValue (int value) {
            this.value = value;
        }

        void setFulfilled () {
            this.isFulfilled = true;
        }

        //toString method for debugging
        @Override
        public String toString() {
            return "Request{" +
                    "requestId=" + requestId +
                    ", key=" + key +
                    ", value=" + value +
                    ", type='" + type + '\'' +
                    ", requesterType='" + requesterType + '\'' +
                    ", requester=" + requester +
                    ", isFulfilled=" + isFulfilled +
                    '}';
        }

    }

    // map of request where the key is the id of the request and the value is the request itself
    // assumption: a cache can receive requests from different clients at the same time
    // therefore we cannot use only a binary switch to check if a request is being processed by the cache
    // we need a map to keep track of all the requests
    private Map<Long, Request> requests = new HashMap<>();

    private HashSet<Integer> recoveredKeys = new HashSet<>();
    private HashMap<ActorRef, Map<Integer, Integer>> recoveredValuesForChild = new HashMap<>();
    private HashSet<ActorRef> childrenToRecover = new HashSet<>();

    // since we use the same class for both types of cache
    // we don't distinguish between different types of children
    // (clients for L2 cache, L2 cache for L1 cache), same for the parent
    // a L2 cache can only have clients as children
    // a L1 cache can only have L2 caches as children
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

        //System.out.println("["+this.type_of_cache+" Cache " + this.id + "] Cache initialized!");
        log.info("[{} CACHE {}] Cache initialized!", this.type_of_cache.toString(), String.valueOf(this.id));
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

        setParent(parent);
        this.database = database;
        setTimeouts(timeouts);

        //System.out.println("["+this.type_of_cache+" Cache " + this.id + "] Cache initialized!");
        log.info("[{} CACHE {}] Cache initialized!", this.type_of_cache.toString(), String.valueOf(this.id));
    }

    static public Props props(int id, String type, ActorRef parent, List<TimeoutConfiguration> timeouts) {
        return Props.create(Cache.class, () -> new Cache(id, type, parent, timeouts));
    }

    static public Props props(int id, String type, ActorRef parent, ActorRef database, List<TimeoutConfiguration> timeouts) {
        return Props.create(Cache.class, () -> new Cache(id, type, parent, database, timeouts));
    }

    private int getID() { return this.id;}

    private TYPE getCacheType() { return this.type_of_cache; }

    // ----------CRASHING LOGIC----------

    public void crash() {
        this.crashed = true;
        clearData();

        //change the behavior of the actor to the crashed one
        getContext().become(crashed());
        log.info("[{} CACHE {}] Cache crashed!", this.type_of_cache.toString(), String.valueOf(this.id));
    }

    public boolean isCrashed(){
        return this.crashed;
    }

    private void recover() {
        this.crashed = false;
        getContext().become(createReceive());
        log.info("[{} CACHE {}] Recovery process started!", this.type_of_cache.toString(), String.valueOf(this.id));
    }

    //----------DATA LOGIC----------

    public void addData(int key, int value) {
        this.data.put(key, value);
    }

    public void addData(Map<Integer, Integer> map) {
        this.data.putAll(map);
    }

    public int getData(int key) {
        return this.data.get(key);
    }

    public Map<Integer, Integer> getData() {
        return this.data;
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

    private boolean hasChildren() { return !this.children.isEmpty(); }

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

    public void setTimeouts(List<TimeoutConfiguration> timeouts)    {
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

    private void startTimeout(String type, long requestId) {
        log.info("[{} CACHE {}] Starting timeout for " + type + " operation, " + getTimeout(type) + " seconds. ", this.type_of_cache.toString(), String.valueOf(this.id));
        getContext().system().scheduler().scheduleOnce(
            Duration.create(getTimeout(type), TimeUnit.SECONDS),
            getSelf(),
            new TimeoutMsg(type, requestId, getParent().path().name()),
            getContext().system().dispatcher(),
            getSelf()
        );
    }

    private void receivedReadRequest(ActorRef requester, String requesterType, ReadRequestMsg msg) {
        Request request = new Request("read", requesterType, requester, msg.getKey(), msg.getRequestId());
        this.requests.put(request.getRequestId(), request);
        log.info("[{} CACHE {}] Put read request in hashmap", this.type_of_cache.toString(), String.valueOf(this.id));
    }

    private void receivedWriteRequest(ActorRef requester, String requesterType, WriteRequestMsg msg) {
        Request request = new Request("write", requesterType, requester, msg.getKey(), msg.getRequestId());
        request.setValue(msg.getValue()); // since it's a write request, we need to set also the value
        this.requests.put(request.getRequestId(), request);
        log.info("[{} CACHE {}] Put write request in hashmap", this.type_of_cache.toString(), String.valueOf(this.id));
    }

    public void sentReadResponse(long requestId) {
        this.requests.get(requestId).setFulfilled();
        log.info("[{} CACHE {}] Set read request as fulfilled", this.type_of_cache.toString(), String.valueOf(this.id));
    }

    public void sentWriteResponse(long requestId) {
        this.requests.get(requestId).setFulfilled();
        log.info("[{} CACHE {}] Set write request as fulfilled", this.type_of_cache.toString(), String.valueOf(this.id));
    }

    private void waitSomeTime(int time) {

        long t0 = System.currentTimeMillis();
        long timeSpent = 0;
        while (timeSpent <= time * 1000L) {
            timeSpent = System.currentTimeMillis() - t0;
        }
    }

    public void addDelayInSeconds(int seconds) {
        try {
            log.info("[{} CACHE {}] Adding delay of {} seconds", this.type_of_cache.toString(), String.valueOf(this.id), String.valueOf(seconds));
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            log.error("[{} CACHE {}] Error while adding delay!", this.type_of_cache.toString(), String.valueOf(this.id));
            e.printStackTrace();
        }
    }

    public void retryRequests(){

        // this business logic is used only by L2 caches
        //TODO: think if we need timeouts also here. db never crash so it could be useless

        System.out.println("inside retryRequests");
        for (Request request : this.requests.values()) {
            System.out.println("request: " + request);
            if (!request.isFulfilled()) {
                log.info("[{} CACHE {}] Retrying request with id {}", this.type_of_cache.toString(), String.valueOf(this.id), String.valueOf(request.getRequestId()));

                // recreate the path
                Stack<ActorRef> path = new Stack<>();
                path.push(request.getRequester()); // requester: client
                path.push(getSelf()); // L2 cache
                System.out.println("recreated path: " + path);

                if (request.getType().equals("read")) {

                    // forward the request to the parent (db)
                    // note that the db could have already been asked the same request from the crashed L1 cache
                    // but the response never arrived to the L2 cache (due to the L1 cache crash), so it's ok to ask again
                    getParent().tell(new ReadRequestMsg(request.getKey(), path, request.getRequestId()), getSelf());
                    log.info("[{} CACHE {}] Forwarded read request to {}", this.type_of_cache.toString(), String.valueOf(this.id), getParent().path().name());
                } else if (request.getType().equals("write")) {

                    // in this case we overwrite the same value in the db
                    // if the db has already been asked the same request from the crashed L1 cache
                    getParent().tell(new WriteRequestMsg(request.getKey(), request.getValue(), path, request.getRequestId()), getSelf());
                    log.info("[{} CACHE {}] Forwarded read request to {}", this.type_of_cache.toString(), String.valueOf(this.id), getParent().path().name());
                } else if (request.getType().equals("crit_read")) {
                    //TODO
                } else if (request.getType().equals("crit_write")) {
                    //TODO
                } else {
                    log.error("[{} CACHE {}] Error: unknown request type {}", this.type_of_cache.toString(), String.valueOf(this.id), request.getType());
                }
            }
        }
    }

    /*-- Actor logic -- */

    public void preStart() {
        //CustomPrint.print(classString, type_of_cache.toString()+" ", String.valueOf(id), " Started!");
        log.info("[{} CACHE {}] Started!", this.type_of_cache.toString(), String.valueOf(this.id));
    }

    // ----------SEND LOGIC----------

    private void onStartInitMsg(Message.StartInitMsg msg){
        sendInitMsg();
    }

    private void sendInitMsg(){
        InitMsg msg = new InitMsg(getSelf(), this.type_of_cache.toString());
        parent.tell(msg, getSelf());
        log.info("[{} CACHE {}] Sent initialization msg to {}", this.type_of_cache.toString(), String.valueOf(this.id), this.parent.path().name());
    }

    private void onDummyMsg(Message.DummyMsg msg){
        //CustomPrint.print(classString, type_of_cache.toString()+" ", String.valueOf(id), " Received dummy msg with payload: " + String.valueOf(msg.getPayload()));
        log.info("[{} CACHE {}] Received dummy msg with payload: {}", this.type_of_cache.toString(), String.valueOf(this.id), String.valueOf(msg.getPayload()));
    }

    // ----------RECEIVE LOGIC----------

    // Here we define the mapping between the received message types and the database methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartInitMsg.class, this::onStartInitMsg)
                .match(InitMsg.class, this::onInitMsg)
                .match(ReadRequestMsg.class, this::onReadRequestMsg)
                .match(ReadResponseMsg.class, this::onReadResponseMsg)
                .match(WriteRequestMsg.class, this::onWriteRequestMsg)
                .match(WriteResponseMsg.class, this::onWriteResponseMsg)
                .match(FillMsg.class, this::onFillMsg)
                .match(DummyMsg.class, this::onDummyMsg)
                // Timeout messages
                .match(RequestConnectionMsg.class, this::onRequestConnectionMsg)
                // Crash and recovery messages
                .match(CrashMsg.class, this::onCrashMsg)
                .match(RecoverMsg.class, this::onRecoverMsg)
                .match(TimeoutMsg.class, this::onTimeoutMsg)
                .match(TimeoutElapsedMsg.class, this::onTimeoutElapsedMsg)
                .match(InfoItemsMsg.class, this::onInfoItemsMsg)
                .match(ResponseConnectionMsg.class, this::onResponseConnectionMsg)
                // Catch all other messages
                .matchAny(o -> log.info("[{} CACHE {}] Received unknown message from {} {}!",
                        getCacheType().toString(), String.valueOf(getID()), getSender().path().name(),
                        o.getClass().getName()))
                .build();
    }

    public Receive crashed() {
        return receiveBuilder()
            .match(RecoverMsg.class, this::onRecoverMsg)
            .matchAny(msg -> {})
            .build();
    }

    // ----------TIMEOUT MESSAGE LOGIC----------

    // Use case: L2 cache receives timeout msg from L1 cache, it means that its L1 is crashed, so it connects to the database
    // Assumption: database is always available, so a cache cannot timeout while waiting for a response from the database
    // L1 cache can timeout while waiting for a response from the L2 cache (advanced case)
    // (if we consider the worst network delay to be less than read timeout)
    private void onTimeoutMsg(TimeoutMsg msg) {

        //TODO: add differentiation type and client inside the timeout msg

        // since a cache can receive multiple requests at the same time
        // check if requestId in hashmap of requests is already fulfilled
        if (this.requests.get(msg.getRequestId()).isFulfilled()) {
            log.info("[{} CACHE {}] Received timeout msg for fulfilled request", getCacheType().toString(), String.valueOf(getID()));
            log.info("[{} CACHE {}] Ignoring timeout msg", getCacheType().toString(), String.valueOf(getID()));
            return;
        }

        switch (msg.getType()) {
            case "read":
                log.info("[{} CACHE {}] Received timeout msg for read request operation", getCacheType().toString(), String.valueOf(getID()));

                // if L2 cache, connect to database
                if(getCacheType().equals(TYPE.L2)){
                    log.info("[{} CACHE {}] Connecting to DATABASE", getCacheType().toString(), String.valueOf(getID()));
                    setParent(getDatabase());
                    getParent().tell(new RequestConnectionMsg("L2"), getSelf());

                    //tell the client that the L1 cache is not available, so this L2 is connecting to the database, so client should wait more, maybe extend the timeout
                    //ActorRef client = msg.getClient();
                    //client.tell(new TimeoutElapsedMsg(), getSelf());
                    //log.info("[{} CACHE {}] Sent timeout elapsed msg to {}", getCacheType().toString(), String.valueOf(getID()), client.path().name());
                }
                break;
            case "write":
                log.info("[{} CACHE {}] Received timeout msg for write request operation", getCacheType().toString(), String.valueOf(getID()));

                // if L2 cache, connect to database
                if(getCacheType().equals(TYPE.L2)){
                    log.info("[{} CACHE {}] Connecting to DATABASE", getCacheType().toString(), String.valueOf(getID()));
                    setParent(getDatabase());
                    getParent().tell(new RequestConnectionMsg("L2"), getSelf());

                    //tell the client that the L1 cache is not available, so this L2 is connecting to the database, so client should wait more, maybe extend the timeout
                    //ActorRef client = msg.getClient();
                    //client.tell(new TimeoutElapsedMsg(), getSelf());
                    //log.info("[{} CACHE {}] Sent timeout elapsed msg to {}", getCacheType().toString(), String.valueOf(getID()), client.path().name());
                }
                break;

            default:
                log.info("[{} CACHE {}] Received timeout msg for unknown operation", getCacheType().toString(), String.valueOf(getID()));
                break;
        }
    }

    private void onRequestConnectionMsg(RequestConnectionMsg msg) {
        log.info("[{} CACHE {}] Received request connection msg from {}", getCacheType().toString(), String.valueOf(getID()), getSender().path().name());
        addChild(getSender());

        //possible improvement: accept only if the children are less than the maximum number of children
        getSender().tell(new ResponseConnectionMsg("ACCEPTED"), getSelf());
        log.info("[{} CACHE {}] Sent response connection msg to {}", getCacheType().toString(), String.valueOf(getID()), getSender().path().name());
    }

    private void onTimeoutElapsedMsg(TimeoutElapsedMsg msg) {}

    // ----------READ MESSAGES LOGIC----------

    public void onReadRequestMsg(ReadRequestMsg readRequestMsg){

        //CustomPrint.print(classString, type_of_cache.toString()+" ", String.valueOf(id), " Received read request msg from " + getSender());
        log.info("[{} CACHE {}] Received read request msg from {}, asking for key {}", getCacheType().toString(), String.valueOf(getID()), getSender().path().name(), readRequestMsg.getKey());

        // check size of path
        // 1 means that the request is coming from a client -> LL: [Client]
        // 2 means that the request is coming from a L2 cache -> LL: [Client, L2_Cache]
        if(readRequestMsg.getPathSize() == 1) {
            receivedReadRequest(getSender(), "client", readRequestMsg);
        } else if (readRequestMsg.getPathSize() == 2) {
            receivedReadRequest(getSender(), "L2", readRequestMsg);
        } else {
            log.error("[{} CACHE {}] Probably wrong route!",  getCacheType().toString(), String.valueOf(getID()));
        }

        log.info("[{} CACHE {}] Request log: {}", getCacheType().toString(), String.valueOf(getID()), this.requests.toString());

        //if data is present
        if (isDataPresent(readRequestMsg.getKey())){

            // check size of path
            // 1 means that the request is coming from a client -> LL: [Client]
            // 2 means that the request is coming from a L2 cache -> LL: [Client, L2_Cache]
            if(readRequestMsg.getPathSize() != 1 && readRequestMsg.getPathSize() != 2){
                //CustomPrint.print(classString, type_of_cache.toString()+" ", String.valueOf(id), " Probably wrong route!");
                log.error("[{} CACHE {}] Probably wrong route!",  getCacheType().toString(), String.valueOf(getID()));
                //TODO: send special error message to client or master
            }

            //response to be sent to child (either client or l2 cache)
            ActorRef child = readRequestMsg.getLast();

            //check if child is between the children of this cache
            if (!getChildren().contains(child)){
                //CustomPrint.print(classString, type_of_cache.toString()+" ", String.valueOf(id), " Child not present in children list!");
                log.error("[{} CACHE {}] Child not present in children list!",  getCacheType().toString(), String.valueOf(getID()));
                //TODO: send special error message to client or master
            }

            int value = getData(readRequestMsg.getKey());

            log.info("[{} CACHE {}] Data is present in cache; key:{}, value:{}", getCacheType().toString(), String.valueOf(getID()), readRequestMsg.getKey(), value);

            ReadResponseMsg response = new ReadResponseMsg(readRequestMsg.getKey(), value, readRequestMsg.getPath(), readRequestMsg.getRequestId());
            child.tell(response, getSelf());
            log.info("[{} CACHE {}] Sent read response msg to {}", getCacheType().toString(), String.valueOf(getID()), child.path().name());

        }
        else { // data not present in cache
            //send read request to parent (L1 cache or database)

            log.info("[{} CACHE {}] Data is not present in cache; key:{}", getCacheType().toString(), String.valueOf(getID()), readRequestMsg.getKey());

            int delay = 1;
            addDelayInSeconds(delay);
            log.info("[{} CACHE {}] Added delay of {} seconds", getCacheType().toString(), String.valueOf(getID()), delay);

            //adding cache to path
            Stack<ActorRef> newPath = new Stack<>();
            newPath.addAll(readRequestMsg.getPath());
            newPath.add(getSelf());

            ReadRequestMsg upperReadRequestMsg = new ReadRequestMsg(readRequestMsg.getKey(), newPath, readRequestMsg.getRequestId());
            getParent().tell(upperReadRequestMsg, getSelf());
            log.info("[{} CACHE {}] Sent read request msg to {}", getCacheType().toString(), String.valueOf(getID()), getParent().path().name());

            startTimeout("read", readRequestMsg.getRequestId());
        }
    }

    //response from database to l1 cache
    //or response from l1 cache to l2 cache
    public void onReadResponseMsg(ReadResponseMsg readResponseMsg){

        //add data to cache
        addData(readResponseMsg.getKey(), readResponseMsg.getValue());
        log.info("[{} CACHE {}] Added data to cache; key:{}, value:{}", getCacheType().toString(), String.valueOf(getID()), readResponseMsg.getKey(), readResponseMsg.getValue());

        //check size of path:
        // 2 means that the response is for a client -> LL:[Client, L2_Cache]
        // 3 means that the response is for a l2 cache -> LL:[Client, L2_Cache, L1_Cache]
        // other values are not allowed

        if(readResponseMsg.getPathSize() != 2 && readResponseMsg.getPathSize() != 3){
            //CustomPrint.print(classString, type_of_cache.toString()+" ", String.valueOf(id), " Probably wrong route!");
            log.info("[{} CACHE {}] Probably wrong route!", getCacheType().toString(), String.valueOf(getID()));
            //TODO: send special error message to client or master
        }

        //create new path without the current cache
        Stack<ActorRef> newPath = new Stack<>();
        newPath.addAll(readResponseMsg.getPath());

        //remove last element from path, which is the current cache
        newPath.pop();

        //child can be a client or a l2 cache
        ActorRef child = newPath.get(newPath.size()-1); //get (not remove) last element from path

        //check if child is between the children of this cache
        if (!getChildren().contains(child)) {
            //CustomPrint.print(classString, type_of_cache.toString() + " ", String.valueOf(id), " Child not present in children list!");
            log.info("[{} CACHE {}] Child not present in children list!", getCacheType().toString(), String.valueOf(getID()));
        }

        //test print msg
        //InfoItemsMsg infoItemsMsg = new InfoItemsMsg();
        //getSelf().tell(infoItemsMsg, getSelf());

        //send response to child
        //either client or l2 cache
        ReadResponseMsg response = new ReadResponseMsg(readResponseMsg.getKey(), readResponseMsg.getValue(), newPath, readResponseMsg.getRequestId());
        child.tell(response, getSelf());
        log.info("[{} CACHE {}] Sent read response msg to {}", getCacheType().toString(), String.valueOf(getID()), child.path().name());

        //System.out.println("TEST, cache" + getCacheType()+ getID()+ readResponseMsg.getRequestId());
        sentReadResponse(readResponseMsg.getRequestId());

        log.info("[{} CACHE {}] Request completed", getCacheType().toString(), String.valueOf(getID()));
        log.info("[{} CACHE {}] All Requests: {}", getCacheType().toString(), String.valueOf(getID()), this.requests.toString());
    }

    // ----------WRITE MESSAGES LOGIC----------

    private void onWriteRequestMsg(WriteRequestMsg writeRequestMsg){

        log.info("[{} CACHE {}] Received write request msg; key:{}, value:{}", getCacheType().toString(), String.valueOf(getID()), writeRequestMsg.getKey(), writeRequestMsg.getValue());

        // check size of path
        // 1 means that the request is coming from a client -> LL: [Client]
        // since a client can only send a request to a L2 cache, we are a L2 cache
        // 2 means that the request is coming from a L2 cache -> LL: [Client, L2_Cache]
        // since a L2 cache can only send a request to a L1 cache, we are a L1 cache
        if(writeRequestMsg.getPathSize() == 1) {
            receivedWriteRequest(getSender(), "client", writeRequestMsg);
        } else if (writeRequestMsg.getPathSize() == 2) {
            receivedWriteRequest(getSender(), "L2", writeRequestMsg);
        } else {
            log.error("[{} CACHE {}] Probably wrong route!",  getCacheType().toString(), String.valueOf(getID()));
        }

        //adding cache to path
        Stack<ActorRef> newPath = new Stack<>();
        newPath.addAll(writeRequestMsg.getPath());
        newPath.add(getSelf());

        WriteRequestMsg upperWriteRequestMsg = new WriteRequestMsg(writeRequestMsg.getKey(), writeRequestMsg.getValue(), newPath, writeRequestMsg.getRequestId());
        log.info("[{} CACHE {}] Path: {}", getCacheType().toString(), String.valueOf(getID()), upperWriteRequestMsg.getPath().toString());

        parent.tell(upperWriteRequestMsg, getSelf());
        log.info("[{} CACHE {}] Sent write msg to {}", getCacheType().toString(), String.valueOf(getID()), getParent().path().name());

        startTimeout("write", upperWriteRequestMsg.getRequestId());

    }

    private void onWriteResponseMsg(WriteResponseMsg writeResponseMsg){

        log.info("[{} CACHE {}] Received write response msg; key:{}, value:{}", getCacheType().toString(), String.valueOf(getID()), writeResponseMsg.getKey(), writeResponseMsg.getValue());

        if (data.containsKey(writeResponseMsg.getKey())){
            data.put(writeResponseMsg.getKey(), writeResponseMsg.getValue());
            log.info("[{} CACHE {}] Added data to cache; key:{}, value:{}", getCacheType().toString(), String.valueOf(getID()), writeResponseMsg.getKey(), writeResponseMsg.getValue());
        }

        System.out.println("TEST, cache " + getCacheType().toString() + " " + getID());
        System.out.println("TEST, cache " + writeResponseMsg.getPathSize());
        System.out.println("TEST, cache " + writeResponseMsg.getPath().toString());

        if(writeResponseMsg.getPathSize() != 2 && writeResponseMsg.getPathSize() != 3){
            log.info("[{} CACHE {}] Probably wrong route!", getCacheType().toString(), String.valueOf(getID()));
            //TODO: send special error message to client or master
        }

        ActorRef destination = writeResponseMsg.getPath().pop();

        // to propagate the response to the path of the request
        destination.tell(writeResponseMsg, getSelf());
        log.info("[{} CACHE {}] Sent write response msg to {}", getCacheType().toString(), String.valueOf(getID()), destination.path().name());

        // to propagate to children in the subtree of the L1 cache used to write the data
        // except to the L2 cache that sent the write request, since it received the WriteResponseMsg
        if (type_of_cache == TYPE.L1) {
            for (ActorRef child : getChildren()) {
                if (!child.equals(destination)) {
                    child.tell(new FillMsg(writeResponseMsg.getKey(), writeResponseMsg.getValue()), getSelf());
                    log.info("[{} CACHE {}] Sent fill msg to {}", getCacheType().toString(), String.valueOf(getID()), child.path().name());
                }
            }
        }

        // set request as fulfilled
        sentWriteResponse(writeResponseMsg.getRequestId());
    }

    private void onFillMsg(FillMsg msg){

        log.info("[{} CACHE {}] Received fill msg; key:{}, value:{}", getCacheType().toString(), String.valueOf(getID()), msg.getKey(), msg.getValue());
        if (this.data.containsKey(msg.getKey())){
            this.data.put(msg.getKey(), msg.getValue());
            log.info("[{} CACHE {}] Added data to cache; key:{}, value:{}", getCacheType().toString(), String.valueOf(getID()), msg.getKey(), msg.getValue());
        }

        // propagate to children
        // TODO: can we assume that if the value is not present in the current cache, it is not present in the children?
        // If that assumption is true, then we must move this in the if statement above
        // L2 caches children are only clients, who are not interested in this message
        // L1 caches children are only L2 caches and we are only interested in this case
        if (this.type_of_cache == TYPE.L1) {
            for (ActorRef child : this.children) {
                FillMsg fillMsg = new FillMsg(msg.getKey(), msg.getValue());
                child.tell(fillMsg, getSelf());
            }
            log.info("[{} CACHE {}] Sent fill msg to cache children", getCacheType().toString(), String.valueOf(getID()));
        }
    }


    // ----------INITIALIZATION MESSAGES LOGIC----------
    private void onInitMsg(Message.InitMsg msg) throws InvalidMessageException{
//        ActorRef tmp = getSender();
//        if (tmp == null){
//            System.out.println("Cache " + id + " received message from null actor");
//            return;
//        }
//        addChild(tmp);
        if ((this.type_of_cache == TYPE.L1 && !Objects.equals(msg.type, "L2")) ||
                (this.type_of_cache == TYPE.L2 && !Objects.equals(msg.type, "client"))){
            throw new InvalidMessageException("Message to wrong destination!");
        }
        addChild(msg.id);
        log.info("[{} CACHE {}] Added {} as a child", getCacheType(), getID(), getSender().path().name());
    }

    private void onInfoMsg (InfoMsg msg){
        log.info("[{} CACHE {}] Parent: {}", getCacheType(), getID(), getParent().path().name());
        log.info("[{} CACHE {}] Children: ", getCacheType(), getID());
        for (ActorRef child : getChildren()) {
            log.info("[{} CACHE {}] {} ", getCacheType(), getID(), child.path().name());
        }
        log.info("[{} CACHE {}] Data: ", getCacheType(), getID());
        for (Map.Entry<Integer, Integer> entry : getData().entrySet()) {
            log.info("[{} CACHE {}] Key = {}, Value = {} ",
                    getCacheType(), getID(), entry.getKey(), entry.getValue());
        }
    }

    // ----------CRASH MESSAGES LOGIC----------
    private void onCrashMsg (Message.CrashMsg msg){
        if (!isCrashed()) {
            crash();
        }
    }

    private void onRecoverMsg (RecoverMsg msg){
        if (isCrashed()) {
            recover();
            // L2 caches does not need to be repopulated with data after crash
            // they will repopulate with data coming from new reads
            if (getCacheType() == TYPE.L1) {
                if (hasChildren()) {
                    for (ActorRef child : getChildren()) {
                        child.tell(new RequestDataRecoverMsg(), getSelf());
                        childrenToRecover.add(child);
                    }
                }
            }
        }
    }

    private void onResponseDataRecoverMsg (ResponseDataRecoverMsg msg){
        childrenToRecover.remove(getSender());
        if (msg.getParent() != getSelf()) {
            children.remove(getSender());
        }

        recoveredKeys.addAll(msg.getData().keySet());
        recoveredValuesForChild.put(getSender(), msg.getData());

        if (childrenToRecover.isEmpty()) {
            getParent().tell(new RequestUpdatedDataMsg(recoveredKeys), getSelf());
        }
    }

    private void onRequestDataRecoverMsg (RequestDataRecoverMsg msg){
        getSender().tell(new ResponseDataRecoverMsg(getData(), getParent()), getSelf());
    }

    private void onResponseUpdatedDataMsg (ResponseUpdatedDataMsg msg){
        addData(msg.getData());

        for (Map.Entry<ActorRef, Map<Integer, Integer>> entry : this.recoveredValuesForChild.entrySet()) {
            ActorRef child = entry.getKey();
            Map<Integer, Integer> tmpData = new HashMap<>();
            for (Map.Entry<Integer, Integer> dataEntry : entry.getValue().entrySet()) {
                int key = dataEntry.getKey();
                int value = dataEntry.getValue();
                if (msg.getData().get(key) != value) {
                    tmpData.put(key, value);
                }
            }

            if (!tmpData.isEmpty()) {
                child.tell(new UpdateDataMsg(tmpData), getSelf());
            }
        }
    }

    private void onUpdateDataMsg (UpdateDataMsg msg){
        addData(msg.getData());
    }

    private void onInfoItemsMsg (InfoItemsMsg msg){
        //log.info("[{} CACHE {}] Data: ", getCacheType(), getID());
        if (getData().size() == 0) {
            log.info("[{} CACHE {}] Data: cache is empty", getCacheType(), getID());
            return;
        }
        for (Map.Entry<Integer, Integer> entry : getData().entrySet()) {
            log.info("[{} CACHE {}] Data ==> Key = {}, Value = {} ",
                    getCacheType(), getID(), entry.getKey(), entry.getValue());
        }
    }

    public void onResponseConnectionMsg(ResponseConnectionMsg msg){

        // this is the case when a L1 cache crashes and so a L2 cache child tries to connect to the database
        // therefore, this business logic will be executed only by L2 caches

        log.info("[{} CACHE {}] Received response connection msg from {}", getCacheType().toString(), String.valueOf(getID()), getSender().path().name());
        if(msg.getResponse().equals("ACCEPTED")){
            log.info("[{} CACHE {}] Connection accepted", getCacheType().toString(), String.valueOf(getID()));

            // since, unlike the clients, caches do not perform only 1 requests at time
            // we must retry all requests that are not yet completed
            // for instance, Client 1 sends a request to L2 Cache 1, which forwards it to L1 Cache 1
            // then, Client 2 sends a request to L2 Cache 1, which forwards it to L1 Cache 1
            // L1 Cache 1 crashes before completing the requests
            // L2 Cache 1 experience a timeout and sends a connection request to the database
            // therefore L2 Cache 1 has two requests to be completed
            retryRequests();
        } else {
            // edge case, not possible in the current implementation
            log.info("[{} CACHE {}] Connection refused", getCacheType().toString(), String.valueOf(getID()));
        }
    }

}
