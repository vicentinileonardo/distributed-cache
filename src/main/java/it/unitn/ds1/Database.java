package it.unitn.ds1;

import akka.actor.*;

import java.util.*;

import it.unitn.ds1.Message.*;

public class Database extends AbstractActor {

    private int id;

    //DEBUG ONLY: assumption that the database is always up
    private boolean crashed = false;

    private Map<Integer, Integer> data = new HashMap<>();

    // since the db and different type of caches have different interactions
    // it is better to have a different set for each type of cache
    private Set<ActorRef> L2_caches = new HashSet<>();
    private Set<ActorRef> L1_caches = new HashSet<>();

    private final HashMap<String, Integer> timeouts = new HashMap<>();

    private Random rnd = new Random();
    private String classString = String.valueOf(getClass());

    public Database(int id, List<TimeoutConfiguration> timeouts) {
        this.id = id;
        setTimeouts(timeouts);
    }

    static public Props props(int id, List<TimeoutConfiguration> timeouts) {
        return Props.create(Database.class, () -> new Database(id, timeouts));
    }

    // ----------L2 CACHES LOGIC----------

    public Set<ActorRef> getL2_caches() {
        return this.L2_caches;
    }

    public void setL2_caches(Set<ActorRef> l2_caches) {
        this.L2_caches = l2_caches;
    }

    public void addL2_cache(ActorRef l2_cache) {
        this.L1_caches.add(l2_cache);
    }

    public void removeL2_cache(ActorRef l2_cache) {
        this.L1_caches.remove(l2_cache);
    }

    public boolean getL2_cache(ActorRef l2_cache) {
        return this.L2_caches.contains(l2_cache);
    }

    // ----------L1 CACHES LOGIC----------

    public Set<ActorRef> getL1_caches() {
        return this.L1_caches;
    }

    public void setL1_caches(Set<ActorRef> l1_caches) {
        this.L1_caches = l1_caches;
    }

    public void addL1_cache(ActorRef l1_cache) {
        this.L1_caches.add(l1_cache);
    }

    public void removeL1_cache(ActorRef l1_cache) {
        this.L1_caches.remove(l1_cache);
    }

    public boolean getL1_cache(ActorRef l1_cache) {
        return this.L1_caches.contains(l1_cache);
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

    @Override
    public void preStart() {

        populateDatabase();

        CustomPrint.print(classString,"",  ""," Started!");
        CustomPrint.print(classString,"",  "", "Initial data in database " + id + ":");
        for (Map.Entry<Integer, Integer> entry : data.entrySet()) {
            CustomPrint.print(classString,"",  "", "Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }

    public void populateDatabase() {
        for (int i = 0; i < 10; i++) {
            data.put(i, rnd.nextInt(200));
        }
        CustomPrint.debugPrint(classString, "Database " + id + " populated");
    }

    public boolean isDataPresent(int key){
        return data.containsKey(key);
    }

    public int getData(int key){
        return data.get(key);
    }

    public void onCacheReadRequestMsg(Message.CacheReadRequestMsg cacheReadRequestMsg){
        CustomPrint.debugPrint(classString, "Database " + id + " received a read request for key " + cacheReadRequestMsg.key + " from client " + cacheReadRequestMsg.client.path().name());

        if (isDataPresent(cacheReadRequestMsg.key)){
            //send response to child (l1 cache) (or l2 cache, if crashes are considered)
            ActorRef child = cacheReadRequestMsg.L1cache; //to be handled in case of crashes
            int value = getData(cacheReadRequestMsg.key);

            Message.CacheReadResponseMsg cacheReadResponseMsg = new Message.CacheReadResponseMsg(cacheReadRequestMsg.key, value, child, cacheReadRequestMsg.L2cache, cacheReadRequestMsg.client);
            child.tell(cacheReadResponseMsg, getSelf());
            CustomPrint.debugPrint(classString, "Database " + id + " read value " + value + " for key " + cacheReadRequestMsg.key);

        } else { // data not present, to decide if the scenario is possible
            //as a matter of fact the client should pick key value that are stored maybe in a config file, to be sure the key is present at least in the database
            //for now, the database will send a response to the client with a value of -1
            ActorRef child = cacheReadRequestMsg.L1cache;
            int value = -1;

            Message.CacheReadResponseMsg cacheReadResponseMsg = new Message.CacheReadResponseMsg(cacheReadRequestMsg.key, value, child, cacheReadRequestMsg.L2cache, cacheReadRequestMsg.client);
            child.tell(cacheReadResponseMsg, getSelf());
            CustomPrint.debugPrint(classString, "Database " + id + " read value " + value + " for key " + cacheReadRequestMsg.key);

        }
    }

    // ----------SENDING LOGIC----------

    private void sendWriteConfirmation(WriteMsg msg, Set<ActorRef> caches) {
        for (ActorRef cache : caches) {
            if (!cache.equals(getSender())){
                cache.tell(new FillMsg(msg.key, msg.value), ActorRef.noSender());
            } else {
                msg.path.pop();
                cache.tell(new WriteConfirmationMsg(msg.key, msg.value, msg.path), ActorRef.noSender());
            }
        }
    }

    // ----------RECEIVE LOGIC----------
    // Here we define the mapping between the received message types and the database methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.InitMsg.class, this::onInitMsg)
                .match(CurrentDataMsg.class, this::onCurrentDataMsg)
                .match(DropDatabaseMsg.class, this::onDropDatabaseMsg)
                .match(CacheReadRequestMsg.class, this::onCacheReadRequestMsg)
                .match(WriteMsg.class, this::onWriteMsg)
                .matchAny(o -> System.out.println("Received unknown message from " + getSender()))
                .build();
    }

    // ----------INITIALIZATION MESSAGES LOGIC----------
    private void onInitMsg(Message.InitMsg msg) throws InvalidMessageException{
//        ActorRef tmp = getSender();
//        if (tmp == null){
//            System.out.println("Cache " + id + " received message from null actor");
//            return;
//        }
//        addL1_cache(tmp);
        if (!Objects.equals(msg.type, "L1")) {
            throw new InvalidMessageException("Message to wrong destination!");
        }
        addL1_cache(msg.id);
        System.out.println("[DATABASE]" +
                " Added " + getSender() + " as a child");
    }

    // ----------READ MESSAGES LOGIC----------


    // ----------WRITE MESSAGES LOGIC----------
    public void onWriteMsg(WriteMsg msg) {
        CustomPrint.debugPrint(classString, "Database " + id + " received a write request for key " + msg.key + " with value " + msg.value);

        data.put(msg.key, msg.value);
        CustomPrint.debugPrint(classString, "Database " + id + " wrote key " + msg.key + " with value " + msg.value);

        // notify all L1 caches
        sendWriteConfirmation(msg, L1_caches);

        // notify all L2 caches that are connected directly with the db
        if (!L2_caches.isEmpty()) {
            sendWriteConfirmation(msg, L2_caches);
        }

    }

    // ----------GENERAL DATABASE MESSAGES LOGIC----------
    public void onCurrentDataMsg(CurrentDataMsg msg) {
        CustomPrint.debugPrint(classString, "Current data in database " + id + ":");
        for (Map.Entry<Integer, Integer> entry : data.entrySet()) {
            CustomPrint.debugPrint(classString, "Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }

    // DEBUG ONLY: assumption is that the database is always up
    public void onDropDatabaseMsg(DropDatabaseMsg msg) {
        CustomPrint.debugPrint(classString, "Database drop request");
        data.clear();
        CustomPrint.debugPrint(classString, "Database " + id + " dropped");
    }
}
