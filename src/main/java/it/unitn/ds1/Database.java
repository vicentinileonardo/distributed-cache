package it.unitn.ds1;

import akka.actor.*;

import java.util.*;
import java.util.concurrent.CompletionStage;

import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.StatusReply;
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
        log.info("[DATABASE] Started!");
        log.info("[DATABASE] Initial data in database : ");
        for (Map.Entry<Integer, Integer> entry : data.entrySet()) {
            log.info("[DATABASE] Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }

    public void populateDatabase() {
        for (int i = 0; i < 10; i++) {
            data.put(i, rnd.nextInt(200));
        }
        log.debug("[DATABASE] Populated!");
    }

    // ----------SENDING LOGIC----------

    private void sendWriteConfirmation(WriteMsg msg, Set<ActorRef> caches) {
        for (ActorRef cache : caches) {
            if (cache.equals(msg.path.get(msg.path.size() - 1))) {
                msg.path.pop();
                getSender().tell(new Status.Success(msg), self());
                log.debug("[DATABASE] Sending write confirmation to " + cache.path().name());
            } else {
                cache.tell(new FillMsg(msg.key, msg.value), getSelf());
                log.debug("[DATABASE] Sending fill message to " + cache.path().name());
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
                .match(WriteMsg.class, this::onWriteMsg)
                .matchAny(o -> log.info("[DATABASE] Received unknown message from "+ getSender()))
                .build();
    }

    // ----------INITIALIZATION MESSAGES LOGIC----------
    private void onInitMsg(Message.InitMsg msg) throws InvalidMessageException{

        if (!Objects.equals(msg.type, "L1")) {
            throw new InvalidMessageException("Message to wrong destination!");
        }
        addL1_cache(msg.id);
        // log.info("[DATABASE] Added L1 cache {} as a child!", getSender().path().name());
    }

    // ----------READ MESSAGES LOGIC----------
    public void onReadRequestMsg(ReadRequestMsg msg) {
        log.debug("[DATABASE] Received read request for key {} from {}", msg.key, getSender().path().name());
        int value = data.get(msg.key);
        log.debug("[DATABASE] Read value {} for key {}", value, msg.key);
        /*
        ReadConfirmationMsg readConfirmationMsg = new ReadConfirmationMsg(msg.key, value, msg.clientID);
        for (ActorRef cache : L2_caches) {
            cache.tell(readConfirmationMsg, getSelf());
        }
        for (ActorRef cache : L1_caches) {
            cache.tell(readConfirmationMsg, getSelf());
        }
        */
    }

    // ----------WRITE MESSAGES LOGIC----------
    public void onWriteMsg(WriteMsg msg) {
        log.debug("[DATABASE] Received write request for key {} with value {} from {}",
                msg.key, msg.value, getSender().path().name());
        data.put(msg.key, msg.value);

        log.debug("[DATABASE] Wrote value {} for key {}", msg.value, msg.key);
        // notify all L1 caches
        log.info("[DATABASE] Send write confirmation to L1 caches");
        sendWriteConfirmation(msg, L1_caches);

        // notify all L2 caches that are connected directly with the db
        if (!L2_caches.isEmpty()) {
            log.info("[DATABASE] Send write confirmation to L2 caches directly connected!");
            sendWriteConfirmation(msg, L2_caches);
        }

    }

    // ----------GENERAL DATABASE MESSAGES LOGIC----------
    public void onCurrentDataMsg(CurrentDataMsg msg) {
        CustomPrint.debugPrint(classString, "","", "Current data in database " + id + ":");
        log.debug("[DATABASE] Current data in database:");
        for (Map.Entry<Integer, Integer> entry : data.entrySet()) {
            log.debug("[DATABASE] Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }

    // DEBUG ONLY: assumption is that the database is always up
    public void onDropDatabaseMsg(DropDatabaseMsg msg) {
        log.debug("[DATABASE] Database drop request!");
        data.clear();
        log.debug("[DATABASE] Dropped database!");
    }
}
