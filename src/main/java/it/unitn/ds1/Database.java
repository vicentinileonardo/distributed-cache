package it.unitn.ds1;

import akka.actor.*;

import java.util.*;

import akka.event.Logging;
import akka.event.LoggingAdapter;

import it.unitn.ds1.Message.*;

public class Database extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

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

        //CustomPrint.print(classString,"",  ""," Started!");
        log.info("[DATABASE " + id + "] Started!");

        //CustomPrint.print(classString,"",  "", "Initial data in database " + id + ":");
        log.info("[DATABASE " + id + "] Initial data in database " + id + ":");
        for (Map.Entry<Integer, Integer> entry : data.entrySet()) {
            //CustomPrint.print(classString,"",  "", "Key = " + entry.getKey() + ", Value = " + entry.getValue());
            log.info("[DATABASE " + id + "] Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }

    public void populateDatabase() {
        for (int i = 0; i < 10; i++) {
            data.put(i, rnd.nextInt(200));
        }
        //CustomPrint.debugPrint(classString, "Database " + id + " populated");
        log.info("[DATABASE " + id + "] Populated");
    }

    public boolean isDataPresent(int key){
        return data.containsKey(key);
    }

    public int getData(int key){
        return data.get(key);
    }

    // ----------SENDING LOGIC----------

    private void sendWriteResponses(WriteRequestMsg writeRequestMsg, Set<ActorRef> caches) {

        for (ActorRef cache : caches) {
            // if the cache is not the sender, send a fill message
            if (!cache.equals(getSender())){
                cache.tell(new FillMsg(writeRequestMsg.getKey(), writeRequestMsg.getValue()), getSelf());
                log.info("[DATABASE " + id + "] Sent a fill message to cache " + cache.path().name());
            } else { // if the cache is the sender, send a write response message
                log.info("[DATABASE " + id + "] Sending a write response message to cache " + cache.path().name());
                Stack<ActorRef> newPath = new Stack<>();
                newPath.addAll(writeRequestMsg.getPath());
                System.out.println("[db, sendWriteResponses] newPath: " + newPath);
                //print last element
                System.out.println("[db, sendWriteResponses] newPath.getLast(): " + newPath.lastElement());

                System.out.println("[db, sendWriteResponses] newPath: " + newPath);
                cache.tell(new WriteResponseMsg(writeRequestMsg.getKey(), writeRequestMsg.getValue(), newPath, writeRequestMsg.getRequestId()), getSelf());
                log.info("[DATABASE " + id + "] Sent a write response message to cache " + cache.path().name());
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
                .match(ReadRequestMsg.class, this::onReadRequestMsg)
                .match(WriteRequestMsg.class, this::onWriteRequestMsg)
                .match(RequestConnectionMsg.class, this::onRequestConnectionMsg)
                .match(RequestUpdatedDataMsg.class, this::onRequestUpdatedDataMsg)
                .matchAny(o -> System.out.println("Database received unknown message from " + getSender()))
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
        log.info("[DATABASE " + id + "] Added " + getSender().path().name() + " as a child");
    }

    private void onRequestConnectionMsg(RequestConnectionMsg msg) {
        if (!msg.getType().isEmpty()){
            if (msg.getType().equals("L2")){
                addL2_cache(getSender());
                log.info("[DATABASE " + id + "] Added " + getSender().path().name() + " as a child");
                getSender().tell(new ResponseConnectionMsg("ACCEPTED"), getSelf());
                log.info("[DATABASE " + id + "] Sent a response connection message to " + getSender().path().name());
            } else if (msg.getType().equals("L1")){
                addL1_cache(getSender());
                log.info("[DATABASE " + id + "] Added " + getSender().path().name() + " as a child");
                getSender().tell(new ResponseConnectionMsg("ACCEPTED"), getSelf());
                log.info("[DATABASE " + id + "] Sent a response connection message to " + getSender().path().name());
            } else
                log.info("[DATABASE " + id + "] Received a request connection message from " + getSender().path().name() + " with invalid type");
        } else {
            log.info("[DATABASE " + id + "] Received a request connection message from " + getSender().path().name() + " with empty type");
        }
    }

    // ----------READ MESSAGES LOGIC----------

    public void onReadRequestMsg(Message.ReadRequestMsg readRequestMsg){
        //CustomPrint.debugPrint(classString, "Database " + id + " received a read request for key " + readRequestMsg.getKey() + " from cache " + getSender().path().name());
        log.info("[DATABASE " + id + "] Received a read request for key " + readRequestMsg.getKey() + " from cache " + getSender().path().name());

        if (isDataPresent(readRequestMsg.getKey())){

            log.info("[DATABASE " + id + "] Data for key " + readRequestMsg.getKey() + " is present in database");

            //send response to child (l1 cache) (or l2 cache, if crashes are considered)
            ActorRef child = readRequestMsg.getLast();

            int value = getData(readRequestMsg.getKey());

            System.out.println("PATH on db!!!! : " + readRequestMsg.getPath());
            ReadResponseMsg readResponseMsg = new ReadResponseMsg(readRequestMsg.getKey(), value, readRequestMsg.getPath(), readRequestMsg.getRequestId());
            child.tell(readResponseMsg, getSelf());
            //CustomPrint.debugPrint(classString, "Database " + id + " read value " + value + " for key " + readRequestMsg.getKey() + " and sent it to cache " + child.path().name());
            log.info("[DATABASE " + id + "] Read value " + value + " for key " + readRequestMsg.getKey() + " and sent it to cache " + child.path().name());

        } else { // data not present, the scenario should not be possible
            //as a matter of fact the client should pick key value that are stored maybe in a config file, to be sure the key is present at least in the database
            //for now, the database will send a response to the client with a value of -1
            ActorRef child = readRequestMsg.getLast();
            int value = -1;

            ReadResponseMsg readResponseMsg = new ReadResponseMsg(readRequestMsg.getKey(), value, readRequestMsg.getPath(), readRequestMsg.getRequestId());
            child.tell(readResponseMsg, getSelf());
            //CustomPrint.debugPrint(classString, "Database " + id + " read value " + value + " for key " + readRequestMsg.getKey());
            log.info("[DATABASE " + id + "] Read value " + value + " for key " + readRequestMsg.getKey());

        }
    }


    // ----------WRITE MESSAGES LOGIC----------
    public void onWriteRequestMsg(WriteRequestMsg msg) {
        log.info("[DATABASE " + id + "] Received a write request for key " + msg.key + " with value " + msg.value);

        data.put(msg.getKey(), msg.getValue());
        log.info("[DATABASE " + id + "] Wrote key " + msg.key + " with value " + msg.value);

        // notify all L1 caches
        sendWriteResponses(msg, L1_caches);
        log.info("[DATABASE " + id + "] Sending write responses to L1 caches");

        // notify all L2 caches that are connected directly with the db (due to previous crashes)
        if (!L2_caches.isEmpty()) {
            sendWriteResponses(msg, L2_caches);
            log.info("[DATABASE " + id + "] Sending write responses ALSO to L2 caches");
        } else {
            log.info("[DATABASE " + id + "] No L2 caches connected directly with the database");
        }

    }

    public void onRequestUpdatedDataMsg(RequestUpdatedDataMsg msg){
        Map<Integer, Integer> tmpData = new HashMap<>();

        // get the values for the keys in the message
        for (Integer key : msg.getKeys()){
            tmpData.put(key, this.data.get(key));
        }

        getSender().tell(new ResponseUpdatedDataMsg(tmpData), getSelf());
        log.info("[DATABASE " + id + "] Sent a response updated data message to " + getSender().path().name());
    }

    // ----------GENERAL DATABASE MESSAGES LOGIC----------
    public void onCurrentDataMsg(CurrentDataMsg msg) {
        //CustomPrint.debugPrint(classString, "Current data in database " + id + ":");
        log.info("[DATABASE " + id + "] Current data in database " + id + ":");
        for (Map.Entry<Integer, Integer> entry : data.entrySet()) {
            //CustomPrint.debugPrint(classString, "Key = " + entry.getKey() + ", Value = " + entry.getValue());
            log.info("[DATABASE " + id + "] Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }

    // DEBUG ONLY: assumption is that the database is always up
    public void onDropDatabaseMsg(DropDatabaseMsg msg) {
        //CustomPrint.debugPrint(classString, "Database drop request");
        log.info("[DATABASE " + id + "] Database drop request");
        data.clear();
        //CustomPrint.debugPrint(classString, "Database " + id + " dropped");
        log.info("[DATABASE " + id + "] Database dropped");
    }
}
