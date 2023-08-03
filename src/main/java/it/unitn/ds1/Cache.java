package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.InvalidMessageException;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.sql.Time;
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
        private ActorRef requester;
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

    //map of request where the key is the id of the request and the value is the request itself
    private Map<Long, Request> requests = new HashMap<>();

    private HashSet<Integer> recoveredKeys = new HashSet<>();
    private HashMap<ActorRef, Map<Integer, Integer>> recoveredValuesForChild = new HashMap<>();
    private HashSet<ActorRef> childrenToRecover = new HashSet<>();

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
        System.out.println("["+this.type_of_cache+" CACHE " + this.id + "]" + " with id " + msg.getRequestId());
        System.out.println("["+this.type_of_cache+" CACHE " + this.id + "]" + " with id(2) " + request.getRequestId()); //this resulted in 0 instead of the same value as above
        this.requests.put(request.getRequestId(), request);
        log.info("[{} CACHE {}] Put read request in hashmap", this.type_of_cache.toString(), String.valueOf(this.id));
    }

    public void sentReadResponse(long requestId) {
        this.requests.get(requestId).setFulfilled();
        log.info("[{} CACHE {}] Set read request as fulfilled", this.type_of_cache.toString(), String.valueOf(this.id));
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
                .match(Message.StartInitMsg.class, this::onStartInitMsg)
                .match(Message.InitMsg.class, this::onInitMsg)
                .match(Message.ReadRequestMsg.class, this::onReadRequestMsg)
                .match(Message.ReadResponseMsg.class, this::onReadResponseMsg)
                .match(Message.WriteMsg.class, this::onWriteMsg)
                .match(Message.WriteConfirmationMsg.class, this::onWriteConfirmationMsg)
                .match(Message.DummyMsg.class, this::onDummyMsg)
                // Timeout messages
                .match(RequestConnectionMsg.class, this::onRequestConnectionMsg)
                // Crash and recovery messages
                .match(CrashMsg.class, this::onCrashMsg)
                .match(RecoverMsg.class, this::onRecoverMsg)
                .match(TimeoutMsg.class, this::onTimeoutMsg)
                .match(TimeoutElapsedMsg.class, this::onTimeoutElapsedMsg)
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

    //Use case: L2 cache receives timeout msg from L1 cache, it means that its L1 is crashed, so it connects to the database
    //assumption: database is always available, so a cache cannot timeout while waiting for a response from the database
    //(if we consider the worst network delay to be less than read timeout)
    private void onTimeoutMsg(TimeoutMsg msg) {

        //TODO: add differentiation type and client inside the timeout msg

        //check if requestid in hashmap of requests is already fulfilled
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

    public void onReadRequestMsg(Message.ReadRequestMsg readRequestMsg){

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

            addDelayInSeconds(20);

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

        //send response to child
        ReadResponseMsg response = new ReadResponseMsg(readResponseMsg.getKey(), readResponseMsg.getValue(), newPath, readResponseMsg.getRequestId());
        child.tell(response, getSelf());
        log.info("[{} CACHE {}] Sent read response msg to {}", getCacheType().toString(), String.valueOf(getID()), child.path().name());

        //System.out.println("TEST, cache" + getCacheType()+ getID()+ readResponseMsg.getRequestId());
        sentReadResponse(readResponseMsg.getRequestId());


        log.info("[{} CACHE {}] Request completed", getCacheType().toString(), String.valueOf(getID()));
        log.info("[{} CACHE {}] All Requests: {}", getCacheType().toString(), String.valueOf(getID()), this.requests.toString());
    }





    // ----------WRITE MESSAGES LOGIC----------
    private void onWriteConfirmationMsg(Message.WriteConfirmationMsg msg){
        ActorRef destination = msg.path.pop();
        destination.tell(msg, getSelf());

        if (data.containsKey(msg.key)){
            data.put(msg.key, msg.value);
        }
    }

    private void onWriteMsg(Message.WriteMsg msg){
        msg.path.push(getSelf());
        CustomPrint.print(classString,
                type_of_cache.toString()+" ",
                String.valueOf(id),
                " Stack: "+msg.path.toString());

        parent.tell(msg, getSelf());
        CustomPrint.print(classString,
                type_of_cache.toString()+" ",
                String.valueOf(id),
                " Sent write msg to " + this.parent);
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
            // L2 caches does not need to be repopulated with data from before crash
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

}
