package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.InvalidMessageException;
import akka.actor.Props;
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
    private Map.Entry<ActorRef, Boolean> response = new AbstractMap.SimpleEntry<>(null, false);
    private final TYPE type_of_cache;

    private Map<Integer, Integer> data = new HashMap<>();

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
        log.info("[{} CACHE {}] Recovery process started!",
                this.type_of_cache.toString(), String.valueOf(this.id));
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

    private boolean hasResponded() { return this.response.getValue(); }

    private ActorRef getRequestSender() { return this.response.getKey(); }

    private void startTimeout(String type) {
        getContext().getSystem().getScheduler().scheduleOnce(
                Duration.create(timeouts.get(type) * 1000, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutMsg(), // the message to send
                getContext().system().dispatcher(),
                getSelf()
        );
    }

    private void sendRequest(ActorRef sender) { this.response = new AbstractMap.SimpleEntry<>(sender, false);}

    public void receivedResponse() { this.response = new AbstractMap.SimpleEntry<>(null, true);}

    private void waitSomeTime(int time) {

        long t0 = System.currentTimeMillis();
        long timeSpent = 0;
        while (timeSpent <= time * 1000L) {
            timeSpent = System.currentTimeMillis() - t0;
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
        Message.InitMsg msg = new Message.InitMsg(getSelf(), this.type_of_cache.toString());
        parent.tell(msg, getSelf());

        String log_msg = "["+this.type_of_cache+" Cache "+this.id+"] Sent initialization msg to " + this.parent;
        System.out.println(log_msg);
    }

    private void onDummyMsg(Message.DummyMsg msg){
        CustomPrint.print(classString, type_of_cache.toString()+" ", String.valueOf(id), " Received dummy msg with payload: " + String.valueOf(msg.getPayload()));
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
    private void onTimeoutMsg(TimeoutMsg msg) {
        if (!hasResponded()) {
            log.info("[{} CACHE {}] Received timeout msg from {}!",
                    getCacheType(), getID(), getSender().path().name());
            receivedResponse();
            log.info("[{} CACHE {}] Connecting to DATABASE",
                    getCacheType().toString(), String.valueOf(getID()));
            setParent(getDatabase());
            getParent().tell(new RequestConnectionMsg("L2"), getSelf());
            getRequestSender().tell(new TimeoutElapsedMsg(), getSelf());
        }
    }

    private void onRequestConnectionMsg(RequestConnectionMsg msg) {
        log.info("[{} CACHE {}] Received request connection msg from {}",
                getCacheType().toString(), String.valueOf(getID()), getSender().path().name());
        addChild(getSender());
        getSender().tell(new ResponseConnectionMsg("ACCEPTED"), getSelf());
    }

    private void onTimeoutElapsedMsg(TimeoutElapsedMsg msg) {}


    // ----------READ MESSAGES LOGIC----------

    public void onReadRequestMsg(Message.ReadRequestMsg readRequestMsg){

        CustomPrint.print(classString, type_of_cache.toString()+" ", String.valueOf(id), " Received read request msg from " + getSender());
        log.info("[{} CACHE {}] Received read request msg from {}",
                getCacheType().toString(), String.valueOf(getID()), getSender().path().name());

        //if data is present
        if (isDataPresent(readRequestMsg.getKey())){

            // check size of path
            // 1 means that the request is coming from a client -> LL: [Client]
            // 2 means that the request is coming from a L2 cache -> LL: [Client, L2_Cache]
            if(readRequestMsg.getPathSize() != 1 && readRequestMsg.getPathSize() != 2){
                CustomPrint.print(classString, type_of_cache.toString()+" ", String.valueOf(id), " Probably wrong route!");
                throw new IllegalArgumentException("Probably wrong route!");
                //TODO: send special error message to client or master
            }

            //send response to child (either client or l2 cache)
            ActorRef child = readRequestMsg.getLast();

            //check if child is between the children of this cache
            if (!getChildren().contains(child)){
                CustomPrint.print(classString, type_of_cache.toString()+" ", String.valueOf(id), " Child not present in children list!");
                throw new IllegalArgumentException("Child not present in children list!");
                //TODO: send special error message to client or master
            }

            int value = getData(readRequestMsg.getKey());

            Message.ReadResponseMsg response = new Message.ReadResponseMsg(readRequestMsg.getKey(), value, readRequestMsg.getPath());
            child.tell(response, getSelf());

        } else { // data not present in cache
            //send cache read request to parent (L1 cache or database)

            //adding cache to path
            Stack<ActorRef> newPath = new Stack<>();
            newPath.addAll(readRequestMsg.getPath());
            newPath.add(getSelf());

            Message.ReadRequestMsg upperReadRequestMsg = new Message.ReadRequestMsg(readRequestMsg.getKey(), newPath);
            getParent().tell(upperReadRequestMsg, getSelf());
        }
    }

    //database to l1 cache
    // or l1 cache to l2 cache
    public void onReadResponseMsg(Message.ReadResponseMsg readResponseMsg){

        //add data to cache
        addData(readResponseMsg.getKey(), readResponseMsg.getValue());

        //check size of path:
        // 2 means that the response is for a client -> LL:[Client, L2_Cache]
        // 3 means that the response is for a l2 cache -> LL:[Client, L2_Cache, L1_Cache]
        // other values are not allowed

        if(readResponseMsg.getPathSize() != 2 && readResponseMsg.getPathSize() != 3){
            CustomPrint.print(classString, type_of_cache.toString()+" ", String.valueOf(id), " Probably wrong route!");
            throw new IllegalArgumentException("Probably wrong route!");
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
            log.info("[{} CACHE {}] Child not present in children list!",
                    getCacheType().toString(), String.valueOf(getID()));
            throw new IllegalArgumentException("Child not present in children list!");
        }

        Message.ReadResponseMsg response = new Message.ReadResponseMsg(readResponseMsg.getKey(), readResponseMsg.getValue(), newPath);
        child.tell(response, getSelf());
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
        String log_msg = "["+this.type_of_cache+" Cache "+this.id+"] Added " + getSender() + " as a child";
        System.out.println(log_msg);
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
