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
                .matchAny(o -> System.out.println("Cache " + id +" received unknown message from " + getSender()))
                .build();
    }

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
