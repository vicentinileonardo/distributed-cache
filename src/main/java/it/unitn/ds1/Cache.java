package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.InvalidMessageException;
import akka.actor.Props;

import java.io.IOException;
import java.util.*;

public class Cache extends AbstractActor{
    private enum TYPE {L1, L2}
    private final int id;
    //DEBUG ONLY: assumption that the cache is always up
    private boolean crashed = false;
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

        System.out.println("["+this.type_of_cache+" Cache " + this.id + "] Cache initialized!");
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


        System.out.println("["+this.type_of_cache+" Cache " + this.id + "] Cache initialized!");
    }

    static public Props props(int id, String type, ActorRef parent, List<TimeoutConfiguration> timeouts) {
        return Props.create(Cache.class, () -> new Cache(id, type, parent, timeouts));
    }

    static public Props props(int id, String type, ActorRef parent, ActorRef database, List<TimeoutConfiguration> timeouts) {
        return Props.create(Cache.class, () -> new Cache(id, type, parent, database, timeouts));
    }

    // ----------CRASHING LOGIC----------

    public void crash() throws IOException {
        this.crashed = true;
        clearData();
        String msg = "["+this.type_of_cache+" Cache "+this.id+"] Crashed!";
        System.out.println(msg);
    }

    public boolean isCrashed(){
        return this.crashed;
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

    /*-- Actor logic -- */

    public void preStart() {

        CustomPrint.print(classString, type_of_cache.toString()+" ", String.valueOf(id), " Started!");
    }

    // ----------SEND LOGIC----------

    private void onStartInitMsg(Message.StartInitMsg msg){
        sendInitMsg();
    }

    private void onDummyMsg(Message.DummyMsg msg){
        CustomPrint.print(classString, type_of_cache.toString()+" ", String.valueOf(id), " Received dummy msg with payload: " + String.valueOf(msg.getPayload()));
    }

    private void sendInitMsg(){
        Message.InitMsg msg = new Message.InitMsg(getSelf(), this.type_of_cache.toString());
        parent.tell(msg, getSelf());

        String log_msg = "["+this.type_of_cache+" Cache "+this.id+"] Sent initialization msg to " + this.parent;
        System.out.println(log_msg);
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
                .matchAny(o -> System.out.println("Cache " + id +" received unknown message from " + getSender()))
                .build();
    }

    // ----------READ MESSAGES LOGIC----------

    public void onReadRequestMsg(Message.ReadRequestMsg readRequestMsg){

        CustomPrint.print(classString, type_of_cache.toString()+" ", String.valueOf(id), " Received read request msg from " + getSender());

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
            CustomPrint.print(classString, type_of_cache.toString() + " ", String.valueOf(id), " Child not present in children list!");
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
}
