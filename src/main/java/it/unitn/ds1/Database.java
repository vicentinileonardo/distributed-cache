package it.unitn.ds1;

import akka.actor.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.event.Logging;
import akka.event.LoggingAdapter;

import it.unitn.ds1.Message.*;
import scala.concurrent.duration.Duration;

public class Database extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private int id;

    private Map<Integer, Integer> data = new HashMap<>();

    // since the db and different type of caches have different interactions
    // it is better to have a different set for each type of cache
    private Set<ActorRef> L2_caches = new HashSet<>();
    private Set<ActorRef> L1_caches = new HashSet<>();

    private final HashMap<String, Integer> timeouts = new HashMap<>();

    private Random rnd = new Random();
    private String classString = String.valueOf(getClass());

    // ----------CRITICAL WRITE SUPPORT DATA STRUCTURES----------

    // critical write, if a critical write for a specific key is ongoing, no critical write for the same key is allowed
    // key -> CriticalWriteRequestMsg
    // we store the original critical write request message to be able to send the response back to the client
    // recovering the path and the request identifier
    private Map<Integer, CriticalWriteRequestMsg> ongoingCritWrites = new HashMap<>();

    // set to store requestId (of type long) of ongoing critical writes
    private Set<Long> ongoingCritWritesRequestId = new HashSet<>();

    // map to store the caches involved in a critical write for a specific key
    // assumption: a cache can be involved in a critical write for a specific key only once at a time
    private Map<Integer, Set<ActorRef>> involvedCachesCritWrites = new HashMap<>();

    // "children" because could be L1 or L2 caches
    private Map<Integer, Set<ActorRef>> childrenToAcceptWriteByKey = new HashMap<>();
    private Map<Integer, Set<ActorRef>> childrenAcceptedWriteByKey = new HashMap<>();
    private Map<Integer, Set<ActorRef>> childrenToConfirmWriteByKey = new HashMap<>();
    private Map<Integer, Set<ActorRef>> childrenConfirmedWriteByKey = new HashMap<>();

    // this map is used to deal with timeouts of accepted write
    // when the operation is accepted but still not confirmed
    private Map<Long, Boolean> acceptedCritWrites = new HashMap<>();


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
        this.L2_caches.add(l2_cache);
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

    public void addDelayInSeconds(int seconds) {
        try {
            log.info("[DATABASE " + id + "] Adding delay of " + seconds + " seconds");
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            log.error("[DATABASE " + id + "] Error while adding delay");
            e.printStackTrace();
        }
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

    private void startTimeout(String type, int key, long requestId) {
        log.info("[DATABASE " + id + "] Starting timeout for " + type + " request " + requestId);
        getContext().system().scheduler().scheduleOnce(
            Duration.create(getTimeout(type), TimeUnit.SECONDS),
            getSelf(),
            new DbTimeoutMsg(type, key, requestId),
            getContext().system().dispatcher(),
            getSelf()
        );
    }


    /*-- Actor logic -- */

    @Override
    public void preStart() {

        populateDatabase();

        log.info("[DATABASE " + id + "] Started!");

        log.info("[DATABASE " + id + "] Initial data in database " + id + ":");
        for (Map.Entry<Integer, Integer> entry : getData().entrySet()) {
            log.info("[DATABASE " + id + "] Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }

    public void populateDatabase() {
        for (int i = 0; i < 20; i++) {
            putData(i, rnd.nextInt(200));
        }
        //CustomPrint.debugPrint(classString, "Database " + id + " populated");
        log.info("[DATABASE " + id + "] Populated");
    }

    public boolean isDataPresent(int key){
        return getData().containsKey(key);
    }

    public int getData(int key){
        return this.data.get(key);
    }

    public void putData(int key, int value){
        this.data.put(key, value);
    }

    public void clearData(){
        this.data.clear();
    }

    public Map<Integer, Integer> getData() {
        return this.data;
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

    private void sendProposedWrite(CriticalWriteRequestMsg criticalWriteRequestMsg, Set<ActorRef> caches) {

        //System.out.println("[db, sendProposedWrite] criticalWriteRequestMsg: " + criticalWriteRequestMsg);
        //System.out.println("[db, sendProposedWrite] caches: " + caches.toString());

        ProposedWriteMsg proposedWriteMsg = new ProposedWriteMsg(criticalWriteRequestMsg.getKey(), criticalWriteRequestMsg.getValue(), criticalWriteRequestMsg.getRequestId());

        for (ActorRef cache : caches) {
             cache.tell(proposedWriteMsg, getSelf());
             log.info("[DATABASE " + id + "] Sent a proposed write message to cache " + cache.path().name());
        }

    }

    private void sendApplyWrite(AcceptedWriteMsg acceptedWriteMsg, Set<ActorRef> caches) {

        System.out.println("[db, sendApplyWrite] acceptedWriteMsg: " + acceptedWriteMsg);
        System.out.println("[db, sendApplyWrite] caches: " + caches.toString());

        log.info("[DATABASE " + id + "] Sending an apply write message to caches");

        ApplyWriteMsg applyWriteMsg = new ApplyWriteMsg(acceptedWriteMsg.getKey(), acceptedWriteMsg.getValue(), acceptedWriteMsg.getRequestId());

        for (ActorRef cache : caches) {
            log.info("[DATABASE " + id + "] Sending an apply write message to cache " + cache.path().name());
            cache.tell(applyWriteMsg, getSelf());
            log.info("[DATABASE " + id + "] Sent an apply write message to cache " + cache.path().name());
        }

        // get key
        int key = acceptedWriteMsg.getKey();

        // get the request id
        long requestId = ongoingCritWrites.get(key).getRequestId();

        // set a timeout, waiting for all confirmed writes
        startTimeout("confirmed_write", key, requestId);
        log.info("[DATABASE " + id + "] Started timeout for confirmed write request " + requestId);

    }


    // ----------RECEIVE LOGIC----------

    // Here we define the mapping between the received message types and the database methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.InitMsg.class, this::onInitMsg)
                .match(CurrentDataMsg.class, this::onCurrentDataMsg)
                .match(DropDatabaseMsg.class, this::onDropDatabaseMsg)
                .match(HealthCheckRequestMsg.class, this::onHealthCheckRequest)

                .match(ReadRequestMsg.class, this::onReadRequestMsg)
                .match(WriteRequestMsg.class, this::onWriteRequestMsg)
                .match(CriticalReadRequestMsg.class, this::onCriticalReadRequestMsg)
                .match(CriticalWriteRequestMsg.class, this::onCriticalWriteRequestMsg)

                .match(AcceptedWriteMsg.class, this::onAcceptedWriteMsg)
                .match(ConfirmedWriteMsg.class, this::onConfirmedWriteMsg)

                .match(DbTimeoutMsg.class, this::onDbTimeoutMsg)

                .match(RequestConnectionMsg.class, this::onRequestConnectionMsg)
                .match(RequestUpdatedDataMsg.class, this::onRequestUpdatedDataMsg)
                .matchAny(o -> System.out.println("Database received unknown message from " + getSender()))
                .build();
    }

    // ----------INITIALIZATION MESSAGE LOGIC----------

    private void onInitMsg(Message.InitMsg msg) throws InvalidMessageException{
//        ActorRef tmp = getSender();
//        if (tmp == null){
//            System.out.println("Cache " + id + " received message from null actor");
//            return;
//        }
//        addL1_cache(tmp);
        if (!Objects.equals(msg.getType(), "L1")) {
            throw new InvalidMessageException("Message to wrong destination!");
        }
        addL1_cache(msg.getId());
        log.info("[DATABASE " + id + "] Added " + getSender().path().name() + " as a child");
    }


    // ----------GENERAL DATABASE MESSAGES LOGIC----------

    public void onCurrentDataMsg(CurrentDataMsg msg) {
        log.info("[DATABASE " + id + "] Current data in database " + id + ":");
        for (Map.Entry<Integer, Integer> entry : getData().entrySet()) {
            log.info("[DATABASE " + id + "] Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }

    // DEBUG ONLY: assumption is that the database is always up
    public void onDropDatabaseMsg(DropDatabaseMsg msg) {
        log.info("[DATABASE " + id + "] Database drop request");
        clearData();
        log.info("[DATABASE " + id + "] Database dropped");
    }

    public void onHealthCheckRequest(HealthCheckRequestMsg msg) {
        HealthCheckResponseMsg new_msg = new HealthCheckResponseMsg(getData());
        getSender().tell(new_msg, getSelf());
    }

    // ----------REQUESTS MESSAGES LOGIC----------

    public void onReadRequestMsg(Message.ReadRequestMsg readRequestMsg){
        log.info("[DATABASE " + id + "] Received a read request for key " + readRequestMsg.getKey() + " from cache " + getSender().path().name());

        int delay = 0;
        addDelayInSeconds(delay);
        log.info("[DATABASE " + id + "] Delayed read request for key " + readRequestMsg.getKey() + " from cache " + getSender().path().name() + " by " + delay + " seconds");

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

        } else { // data not present
            //for now, the database will send a response to the client with a value of -1
            ActorRef child = readRequestMsg.getLast();
            int value = -1;

            ReadResponseMsg readResponseMsg = new ReadResponseMsg(readRequestMsg.getKey(), value, readRequestMsg.getPath(), readRequestMsg.getRequestId());
            child.tell(readResponseMsg, getSelf());
            //CustomPrint.debugPrint(classString, "Database " + id + " read value " + value + " for key " + readRequestMsg.getKey());
            log.info("[DATABASE " + id + "] Read value " + value + " for key " + readRequestMsg.getKey());

        }
    }

    public void onWriteRequestMsg(WriteRequestMsg msg) {
        log.info("[DATABASE " + id + "] Received a write request for key " + msg.getKey() + " with value " + msg.getValue() + " from cache " + getSender().path().name());

        putData(msg.getKey(), msg.getValue());
        log.info("[DATABASE " + id + "] Wrote key " + msg.getKey() + " with value " + msg.getValue());

        int delay = 10;
        addDelayInSeconds(delay);
        log.info("[DATABASE " + id + "] Delayed write request for key " + msg.getKey() + " from cache " + getSender().path().name() + " by " + delay + " seconds");

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

    public void onCriticalReadRequestMsg(CriticalReadRequestMsg criticalReadRequestMsg){
        log.info("[DATABASE " + id + "] Received a critical read request for key " + criticalReadRequestMsg.getKey() + " from cache " + getSender().path().name());

        if (isDataPresent(criticalReadRequestMsg.getKey())){

            log.info("[DATABASE " + id + "] Data for key " + criticalReadRequestMsg.getKey() + " is present in database");

            //send response to child (l1 cache) (or l2 cache, if crashes are considered)
            ActorRef child = criticalReadRequestMsg.getLast();

            int value = getData(criticalReadRequestMsg.getKey());

            CriticalReadResponseMsg criticalReadResponseMsg = new CriticalReadResponseMsg(criticalReadRequestMsg.getKey(), value, criticalReadRequestMsg.getPath(), criticalReadRequestMsg.getRequestId());
            child.tell(criticalReadResponseMsg, getSelf());
            log.info("[DATABASE " + id + "] Read value " + value + " for key " + criticalReadRequestMsg.getKey() + " and sent it to cache " + child.path().name());

        } else { // data not present
            //for now, the database will send a response to the client with a value of -1
            ActorRef child = criticalReadRequestMsg.getLast();
            int value = -1;

            CriticalReadResponseMsg criticalReadResponseMsg = new CriticalReadResponseMsg(criticalReadRequestMsg.getKey(), value, criticalReadRequestMsg.getPath(), criticalReadRequestMsg.getRequestId());
            child.tell(criticalReadResponseMsg, getSelf());
            log.info("[DATABASE " + id + "] Read value " + value + " for key " + criticalReadRequestMsg.getKey() + " and sent it to cache " + child.path().name());

        }
    }

    public void onCriticalWriteRequestMsg(CriticalWriteRequestMsg criticalWriteRequestMsg) {
        log.info("[DATABASE " + id + "] Received a critical write request for key " + criticalWriteRequestMsg.getKey() + " with value " + criticalWriteRequestMsg.getValue() + " from cache " + getSender().path().name());

        int delay = 0;
        addDelayInSeconds(delay);
        log.info("[DATABASE " + id + "] Delayed critical write request for key " + criticalWriteRequestMsg.getKey() + " from cache " + getSender().path().name() + " by " + delay + " seconds");

        // check if the key is already present in ongoingCritWrites
        // this could also be a use case for retryRequest from a L2 cache connecting directly to the database
        if (ongoingCritWrites.containsKey(criticalWriteRequestMsg.getKey())) {
            // the critical write will NOT be accepted
            log.info("[DATABASE " + id + "] Critical write for key " + criticalWriteRequestMsg.getKey() + " with value " + criticalWriteRequestMsg.getValue() + " will NOT be accepted");

            // send write response with value isRefused = true
            CriticalWriteResponseMsg criticalWriteResponseMsg = new CriticalWriteResponseMsg(criticalWriteRequestMsg.getKey(), criticalWriteRequestMsg.getValue(), criticalWriteRequestMsg.getPath(), criticalWriteRequestMsg.getRequestId(), true);
            getSender().tell(criticalWriteResponseMsg, getSelf());
            log.info("[DATABASE " + id + "] Sending write response to cache " + getSender().path().name());
            return;
        }

        ongoingCritWrites.put(criticalWriteRequestMsg.getKey(), criticalWriteRequestMsg);
        log.info("[DATABASE " + id + "] Added critical write for key " + criticalWriteRequestMsg.getKey() + " with value " + criticalWriteRequestMsg.getValue() + " to ongoingCritWrites");

        ongoingCritWritesRequestId.add(criticalWriteRequestMsg.getRequestId());
        log.info("[DATABASE " + id + "] Added critical write request id " + criticalWriteRequestMsg.getRequestId() + " to ongoingCritWritesRequestId");

        // send proposed write to all connected caches

        //print caches
        for (ActorRef cache : L1_caches) {
            log.info("[DATABASE " + id + "] L1 cache, onCriticalWriteRequestMsg: " + cache.path().name());
        }

        //create a set of caches that will accept the critical write, starting from the L1 caches
        Set<ActorRef> cachesToAcceptWrite = new HashSet<>(L1_caches);

        sendProposedWrite(criticalWriteRequestMsg, cachesToAcceptWrite);
        childrenToAcceptWriteByKey.put(criticalWriteRequestMsg.getKey(), cachesToAcceptWrite);
        log.info("[DATABASE " + id + "] Sending proposed write to L1 caches");

        // send also to all L2 caches that are connected directly with the db (due to previous crashes)
        if (!L2_caches.isEmpty()) {
            sendProposedWrite(criticalWriteRequestMsg, L2_caches);

            Set<ActorRef> newSet = childrenToAcceptWriteByKey.get(criticalWriteRequestMsg.getKey());
            newSet.addAll(L2_caches);
            childrenToAcceptWriteByKey.put(criticalWriteRequestMsg.getKey(), newSet);

            log.info("[DATABASE " + id + "] Sending write responses ALSO to L2 caches");
        } else {
            log.info("[DATABASE " + id + "] No L2 caches connected directly with the database");
        }


        // get key
        int key = criticalWriteRequestMsg.getKey();

        // get request id
        long requestId = criticalWriteRequestMsg.getRequestId();

        // set a timeout, waiting for the accepted write messages
        startTimeout("accepted_write", key, requestId);
        log.info("[DATABASE " + id + "] Started timeout for proposed write request " + requestId);

    }

    // ----------CRITICAL WRITE FINAL PHASE MESSAGES LOGIC----------

    public void onAcceptedWriteMsg(AcceptedWriteMsg acceptedWriteMsg) {

        //System.out.println("[database] beginning, before sendApplyWrite to L1, caches: " + L1_caches);

        log.info("[DATABASE " + id + "] Received accepted write msg; key:{}, value:{} from {}", acceptedWriteMsg.getKey(), acceptedWriteMsg.getValue(), getSender().path().name());

        // check if the related crit_write is still going on
        if(!ongoingCritWritesRequestId.contains(acceptedWriteMsg.getRequestId())){
            log.info("[DATABASE " + id + "] Received accepted write msg for request id " + acceptedWriteMsg.getRequestId() + " but the related critical write is not ongoing anymore");
            return;
        }


        //System.out.println("[database] before removal, L1 caches: " + L1_caches);
        //System.out.println("[database] before removal , childrenToAcceptWriteByKey: " + childrenToAcceptWriteByKey.toString());
        // remove from the set of caches that must respond to this cache
        childrenToAcceptWriteByKey.get(acceptedWriteMsg.getKey()).remove(getSender());

        //System.out.println("[database] after removal, childrenToAcceptWriteByKey: " + childrenToAcceptWriteByKey.toString());
        //System.out.println("[database] after removal, L1 caches: " + L1_caches);

        childrenAcceptedWriteByKey.putIfAbsent(acceptedWriteMsg.getKey(), new HashSet<ActorRef>());
        childrenAcceptedWriteByKey.get(acceptedWriteMsg.getKey()).add(getSender());

        // check every time if all caches have responded for that specific key
        if (childrenToAcceptWriteByKey.get(acceptedWriteMsg.getKey()).isEmpty()) {
            log.info("[DATABASE " + id + "] All caches have responded (accepted write) for key: {}", acceptedWriteMsg.getKey());

            // here is a good place to add data to the database
            putData(acceptedWriteMsg.getKey(), acceptedWriteMsg.getValue());
            log.info("[DATABASE " + id + "] Added data to database: key:{}, value:{}", acceptedWriteMsg.getKey(), acceptedWriteMsg.getValue());

            acceptedCritWrites.put(acceptedWriteMsg.getRequestId(), true);


            // send an apply write to all connected caches

            //print caches
            for (ActorRef cache : L1_caches) {
                log.info("[DATABASE " + id + "] L1 cache, onAcceptedWriteMsg: " + cache.path().name());
            }

            System.out.println("[database] before sendApplyWrite to L1");
            System.out.println("[database] before sendApplyWrite to L1, caches: " + L1_caches.toString());

            Set<ActorRef> cachesToConfirmWrite = new HashSet<>(L1_caches);

            sendApplyWrite(acceptedWriteMsg, cachesToConfirmWrite);
            childrenToConfirmWriteByKey.put(acceptedWriteMsg.getKey(), cachesToConfirmWrite);
            log.info("[DATABASE " + id + "] Sending apply write to L1 caches");

            // send also to all L2 caches that are connected directly with the db (due to previous crashes)
            if (!L2_caches.isEmpty()) {
                System.out.println("[database] before sendApplyWrite to L2");
                sendApplyWrite(acceptedWriteMsg, L2_caches);

                Set<ActorRef> newSet = childrenToConfirmWriteByKey.get(acceptedWriteMsg.getKey());
                newSet.addAll(L2_caches);
                childrenToConfirmWriteByKey.put(acceptedWriteMsg.getKey(), newSet);

                log.info("[DATABASE " + id + "] Sending apply write ALSO to L2 caches");
            } else {
                log.info("[DATABASE " + id + "] No L2 caches connected directly with the database to send apply write");
            }

        }
        else {
            log.info("[DATABASE " + id + "] Not all caches have responded yet (accepted write) for key: {}", acceptedWriteMsg.getKey());
            log.info("[DATABASE " + id + "] Still waiting for responses from: {}", childrenToAcceptWriteByKey.get(acceptedWriteMsg.getKey()).toString());
        }

    }

    public void onConfirmedWriteMsg(ConfirmedWriteMsg confirmedWriteMsg) {
        log.info("[DATABASE " + id + "] Received confirmed write msg; key:{}, value:{} from {}", confirmedWriteMsg.getKey(), confirmedWriteMsg.getValue(), getSender().path().name());

        // check if the related crit_write is still going on
        if(!ongoingCritWritesRequestId.contains(confirmedWriteMsg.getRequestId())){
            log.info("[DATABASE " + id + "] Received confirmed write msg for request id " + confirmedWriteMsg.getRequestId() + " but the related critical write is not ongoing anymore");
            return;
        }

        //System.out.println("[database] before removal , childrenToConfirmWriteByKey: " + childrenToConfirmWriteByKey.toString());
        // remove from the set of caches that must respond to this cache
        childrenToConfirmWriteByKey.get(confirmedWriteMsg.getKey()).remove(getSender());

        //System.out.println("[database] after removal, childrenToConfirmWriteByKey: " + childrenToConfirmWriteByKey.toString());

        childrenConfirmedWriteByKey.putIfAbsent(confirmedWriteMsg.getKey(), new HashSet<ActorRef>());
        childrenConfirmedWriteByKey.get(confirmedWriteMsg.getKey()).add(getSender());


        System.out.println("[database] before first involvedcachescritwrites: " + involvedCachesCritWrites.toString());
        involvedCachesCritWrites.put(confirmedWriteMsg.getKey(), childrenConfirmedWriteByKey.get(confirmedWriteMsg.getKey()));
        System.out.println("[database] after first involvedcachescritwrites: " + involvedCachesCritWrites.toString());

        //add L2 caches to involvedCachesCritWrites
        log.info("[DATABASE " + id + "] Caches INVOLVED" + confirmedWriteMsg.getCaches());

        // check every time if all caches have responded for that specific key
        if (childrenToConfirmWriteByKey.get(confirmedWriteMsg.getKey()).isEmpty()) {
            log.info("[DATABASE " + id + "] All caches have responded (confirmed write) for key: {}", confirmedWriteMsg.getKey());

            // get requestId
            long requestId = ongoingCritWrites.get(confirmedWriteMsg.getKey()).getRequestId();

            // get the value of the key from ongoingCritWrites, and remove the key from ongoingCritWrites
            CriticalWriteRequestMsg criticalWriteRequestMsg = ongoingCritWrites.remove(confirmedWriteMsg.getKey());

            //System.out.println("[database] confirmed get key" + confirmedWriteMsg.getKey());
            Set<ActorRef> involvedCaches = involvedCachesCritWrites.remove(confirmedWriteMsg.getKey());
            //System.out.println("[database] 2ND! involvedCachesCritWrites, after removal: " +involvedCaches.toString());

            ongoingCritWritesRequestId.remove(requestId);
            log.info("[DATABASE " + id + "] Removed request id from ongoingCritWritesRequestId: key: {}", confirmedWriteMsg.getKey());

            // remove from ongoingCritWrites
            ongoingCritWrites.remove(confirmedWriteMsg.getKey());
            log.info("[DATABASE " + id + "] Removed key from ongoingCritWrites: key: {}", confirmedWriteMsg.getKey());
            log.info("[DATABASE " + id + "] Ongoing critical writes: " + ongoingCritWrites.toString());

            CriticalWriteResponseMsg criticalWriteResponseMsg = new CriticalWriteResponseMsg(confirmedWriteMsg.getKey(), confirmedWriteMsg.getValue(), criticalWriteRequestMsg.getPath(), criticalWriteRequestMsg.getRequestId(), false);
            criticalWriteResponseMsg.setUpdatedCaches(involvedCaches);

            log.info("[DATABASE " + id + "] involvedCaches: " + involvedCaches.toString());
            log.info("[DATABASE " + id + "] criticalWriteResponseMsg CACHES: " + criticalWriteResponseMsg.printUpdatedCaches());

            ActorRef child = criticalWriteRequestMsg.getLast();
            // get the path to the cache that requested the critical write using ongoingCritWrites
            child.tell(criticalWriteResponseMsg, getSelf());

            log.info("[DATABASE " + id + "] Sending write response to cache, GET_LAST (CHILD)" + child.path().name());

        }
        else {
            log.info("[DATABASE " + id + "] Not all caches have responded yet (confirmed write) for key: {}", confirmedWriteMsg.getKey());
            log.info("[DATABASE " + id + "] Still waiting for responses from: {}", childrenToConfirmWriteByKey.get(confirmedWriteMsg.getKey()).toString());
        }
    }

    // ----------DB TIMEOUT MESSAGES LOGIC----------

    private void onDbTimeoutMsg(DbTimeoutMsg msg) {

        // check if request is already fulfilled
        if (!this.ongoingCritWritesRequestId.contains(msg.getRequestId())) {
            log.info("[DATABASE " + id + "] Received timeout msg for critical write request operation");
            log.info("[DATABASE " + id + "] Ignoring timeout msg for critical write request operation since it has already been fulfilled");
            return;
        }

        // if operation is not fulfilled yet

        switch (msg.getType()) {
            case "accepted_write":

                // received either from L1 cache (default) or L2 cache (connected)
                log.info("[DATABASE " + id + "] Received timeout msg for accepted write operation");

                if(acceptedCritWrites.get(msg.getRequestId()) != null){
                    if (acceptedCritWrites.get(msg.getRequestId())){
                        log.info("[DATABASE " + id + "] Ignoring timeout msg for accepted write since crit_write has been accepted but it is still ongoing");
                        return;
                    }
                } else {
                    log.info("[DATABASE " + id + "] Crit write has not been accepted yet");
                }

                // crit_write will be aborted
                // database did not addData
                // some caches have accepted the crit_write
                // so they have tmpWriteData for that key
                // and they will have cleared the value on data hashmap, unnecessary data loss (tradeoff)
                // we need to clear tmpWriteData for that key to allow future crit_writes on that key

                // either the caches receive the DropTmpWriteDataMsg and clear tmpWriteData (reliable FIFO channels)
                // or they have crashed and lose by itself the tmpWriteData

                // loop through all L1 children
                for (ActorRef child : L1_caches) {
                    DropTmpWriteDataMsg dropTmpWriteDataMsg = new DropTmpWriteDataMsg(msg.getKey());
                    child.tell(dropTmpWriteDataMsg, getSelf());
                    log.info("[DATABASE " + id + "] Sent drop tmp write data msg for key: {} to cache: {}", msg.getKey(), child.path().name());
                }
                for (ActorRef child : L2_caches) {
                    DropTmpWriteDataMsg dropTmpWriteDataMsg = new DropTmpWriteDataMsg(msg.getKey());
                    child.tell(dropTmpWriteDataMsg, getSelf());
                    log.info("[DATABASE " + id + "] Sent drop tmp write data msg for key: {} to cache: {} (L2 cache connected directly)", msg.getKey(), child.path().name());
                }


                log.info("[DATABASE " + id + "] Aborting critical write operation for key: {} and request id: {}", msg.getKey(), msg.getRequestId());

                // crit_write abortion procedure

                CriticalWriteRequestMsg refusedCriticalWriteRequestMsg = ongoingCritWrites.remove(msg.getKey());
                ActorRef child_1 = refusedCriticalWriteRequestMsg.getLast();

                // send write response with value isRefused = true
                CriticalWriteResponseMsg refusedCriticalWriteResponseMsg = new CriticalWriteResponseMsg(refusedCriticalWriteRequestMsg.getKey(), refusedCriticalWriteRequestMsg.getValue(), refusedCriticalWriteRequestMsg.getPath(), refusedCriticalWriteRequestMsg.getRequestId(), true);

                ongoingCritWritesRequestId.remove(msg.getRequestId());
                involvedCachesCritWrites.remove(msg.getKey());
                childrenToAcceptWriteByKey.remove(msg.getKey());
                childrenAcceptedWriteByKey.remove(msg.getKey());
                childrenToConfirmWriteByKey.remove(msg.getKey());
                childrenConfirmedWriteByKey.remove(msg.getKey());

                child_1.tell(refusedCriticalWriteResponseMsg, getSelf());
                log.info("[DATABASE " + id + "] Sending (NOT FULFILLED) write response to cache" + child_1.path().name());

                break;
            case "confirmed_write":
                // received either from L1 cache (default) or L2 cache (connected)
                log.info("[DATABASE " + id + "] Received timeout msg for confirmed write operation");

                // since at this point we received all accepted write responses
                // we are sure that all caches have cleared the old value for the key involved
                // we are also sure that tmpWriteData for that key has been cleared
                // so the crit_write requirement is satisfied
                // but maybe some caches did not update the new value (not a problem, since it is not a requirement)

                // send crit write response
                log.info("[DATABASE " + id + "] Aborting critical write operation for key: {} and request id: {}", msg.getKey(), msg.getRequestId());

                // crit_write abortion procedure

                CriticalWriteRequestMsg acceptedCriticalWriteRequestMsg = ongoingCritWrites.remove(msg.getKey());
                ActorRef child_2 = acceptedCriticalWriteRequestMsg.getLast();

                // send write response with value isRefused = true
                CriticalWriteResponseMsg acceptedCriticalWriteResponseMsg = new CriticalWriteResponseMsg(acceptedCriticalWriteRequestMsg.getKey(), acceptedCriticalWriteRequestMsg.getValue(), acceptedCriticalWriteRequestMsg.getPath(), acceptedCriticalWriteRequestMsg.getRequestId(), false);

                ongoingCritWritesRequestId.remove(msg.getRequestId());
                involvedCachesCritWrites.remove(msg.getKey());
                childrenToAcceptWriteByKey.remove(msg.getKey());
                childrenAcceptedWriteByKey.remove(msg.getKey());
                childrenToConfirmWriteByKey.remove(msg.getKey());
                childrenConfirmedWriteByKey.remove(msg.getKey());

                child_2.tell(acceptedCriticalWriteResponseMsg, getSelf());
                log.info("[DATABASE " + id + "] Sending (FULFILLED) write response to cache" + child_2.path().name());

                break;
            default:
                log.info("[DATABASE " + id + "] Received timeout msg for unknown operation");
                break;
        }
    }

    // ----------RECOVERY PROCEDURE MESSAGE LOGIC----------

    public void onRequestUpdatedDataMsg(RequestUpdatedDataMsg msg){
        Map<Integer, Integer> tmpData = new HashMap<>();

        // get the values for the keys in the message
        for (Integer key : msg.getKeys()){
            System.out.println("[database] onRequestUpdatedDataMsg, key: " + key);
            tmpData.put(key, getData(key));
        }

        getSender().tell(new ResponseUpdatedDataMsg(tmpData), getSelf());
        log.info("[DATABASE " + id + "] Sent a response updated data message to " + getSender().path().name());
    }

    // ----------CONNECTION MESSAGE LOGIC----------

    private void onRequestConnectionMsg(RequestConnectionMsg msg) {
        if (!msg.getType().isEmpty()){
            if (msg.getType().equals("L2")){
                addL2_cache(getSender());
                System.out.println("[db, onRequestConnectionMsg] added l2");
                log.info("[DATABASE " + id + "] Added " + getSender().path().name() + " as a child");
                getSender().tell(new ResponseConnectionMsg("ACCEPTED"), getSelf());
                log.info("[DATABASE " + id + "] Sent a response connection message to " + getSender().path().name());

                //print l2 caches
                System.out.println("[db, onRequestConnectionMsg] l2 caches: " + getL2_caches().toString());

                //print l1 caches
                System.out.println("[db, onRequestConnectionMsg] l1 caches: " + getL1_caches().toString());

            } else if (msg.getType().equals("L1")){ // for now, should never happen
                addL1_cache(getSender());
                System.out.println("[db, onRequestConnectionMsg] added l1");
                log.info("[DATABASE " + id + "] Added " + getSender().path().name() + " as a child");
                getSender().tell(new ResponseConnectionMsg("ACCEPTED"), getSelf());
                log.info("[DATABASE " + id + "] Sent a response connection message to " + getSender().path().name());
            } else
                log.info("[DATABASE " + id + "] Received a request connection message from " + getSender().path().name() + " with invalid type");
        } else {
            log.info("[DATABASE " + id + "] Received a request connection message from " + getSender().path().name() + " with empty type");
        }
    }

}
