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

    // critical write, if a critical write for a specific key is ongoing, no write for the same key is allowed
    // key -> CriticalWriteRequestMsg
    // we store the original critical write request message to be able to send the response back to the client
    // recovering the path and the request identifier
    private Map<Integer, CriticalWriteRequestMsg> ongoingCritWrites = new HashMap<>();

    // map to store the caches involved in a critical write for a specific key
    // assumption: a cache can be involved in a critical write for a specific key only once at a time
    // TODO: check is this behaviour is already guaranteed
    private Map<Integer, Set<ActorRef>> involvedCachesCritWrites = new HashMap<>();


    private Map<Integer, Set<ActorRef>> childrenToAcceptWriteByKey = new HashMap<>();
    private Map<Integer, Set<ActorRef>> childrenAcceptedWriteByKey = new HashMap<>();
    private Map<Integer, Set<ActorRef>> childrenToConfirmWriteByKey = new HashMap<>();
    private Map<Integer, Set<ActorRef>> childrenConfirmedWriteByKey = new HashMap<>();

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

    private void sendProposedWrite(CriticalWriteRequestMsg criticalWriteRequestMsg, Set<ActorRef> caches) {

        ProposedWriteMsg proposedWriteMsg = new ProposedWriteMsg(criticalWriteRequestMsg.getKey(), criticalWriteRequestMsg.getValue());

        for (ActorRef cache : caches) {
             cache.tell(proposedWriteMsg, getSelf());
             log.info("[DATABASE " + id + "] Sent a proposed write message to cache " + cache.path().name());
        }

        // set a timeout
    }

    private void sendApplyWrite(AcceptedWriteMsg acceptedWriteMsg, Set<ActorRef> caches) {

        log.info("[DATABASE " + id + "] Sending an apply write message to caches");

        ApplyWriteMsg applyWriteMsg = new ApplyWriteMsg(acceptedWriteMsg.getKey(), acceptedWriteMsg.getValue());

        for (ActorRef cache : caches) {
            log.info("[DATABASE " + id + "] Sending an apply write message to cache " + cache.path().name());
            cache.tell(applyWriteMsg, getSelf());
            log.info("[DATABASE " + id + "] Sent an apply write message to cache " + cache.path().name());
        }

        // set a timeout
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
                .match(CriticalReadRequestMsg.class, this::onCriticalReadRequestMsg)
                .match(CriticalWriteRequestMsg.class, this::onCriticalWriteRequestMsg)

                .match(AcceptedWriteMsg.class, this::onAcceptedWriteMsg)
                .match(ConfirmedWriteMsg.class, this::onConfirmedWriteMsg)

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

        } else { // data not present, the scenario should not be possible
            //as a matter of fact the client should pick key value that are stored maybe in a config file, to be sure the key is present at least in the database
            //for now, the database will send a response to the client with a value of -1
            ActorRef child = criticalReadRequestMsg.getLast();
            int value = -1;

            CriticalReadResponseMsg criticalReadResponseMsg = new CriticalReadResponseMsg(criticalReadRequestMsg.getKey(), value, criticalReadRequestMsg.getPath(), criticalReadRequestMsg.getRequestId());
            child.tell(criticalReadResponseMsg, getSelf());
            log.info("[DATABASE " + id + "] Read value " + value + " for key " + criticalReadRequestMsg.getKey() + " and sent it to cache " + child.path().name());

        }
    }

    public void onCriticalWriteRequestMsg(CriticalWriteRequestMsg criticalWriteRequestMsg) {
        log.info("[DATABASE " + id + "] Received a critical write request for key " + criticalWriteRequestMsg.getKey() + " with value " + criticalWriteRequestMsg.getKey());

        // check if the key is already present in ongoingCritWrites
        if (ongoingCritWrites.containsKey(criticalWriteRequestMsg.getKey())) {
            // the critical write will not be accepted
            log.info("[DATABASE " + id + "] Critical write for key " + criticalWriteRequestMsg.getKey() + " with value " + criticalWriteRequestMsg.getValue() + " will not be accepted");

            // send write response with value isRefused = true
            CriticalWriteResponseMsg criticalWriteResponseMsg = new CriticalWriteResponseMsg(criticalWriteRequestMsg.getKey(), criticalWriteRequestMsg.getValue(), criticalWriteRequestMsg.getPath(), criticalWriteRequestMsg.getRequestId(), true);
            getSender().tell(criticalWriteResponseMsg, getSelf());
            log.info("[DATABASE " + id + "] Sending write response to cache " + getSender().path().name());

        }

        ongoingCritWrites.put(criticalWriteRequestMsg.getKey(), criticalWriteRequestMsg);

        // send proposed write to all connected caches

        //print caches
        for (ActorRef cache : L1_caches) {
            log.info("[DATABASE " + id + "] L1 cache, onCriticalWriteRequestMsg: " + cache.path().name());
        }

        sendProposedWrite(criticalWriteRequestMsg, L1_caches);
        childrenToAcceptWriteByKey.put(criticalWriteRequestMsg.getKey(), L1_caches);
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

    }

    public void onAcceptedWriteMsg(AcceptedWriteMsg acceptedWriteMsg) {
        log.info("[DATABASE " + id + "] Received accepted write msg; key:{}, value:{} from {}", acceptedWriteMsg.getKey(), acceptedWriteMsg.getValue(), getSender().path().name());

        System.out.println("[database] before removal , childrenToAcceptWriteByKey: " + childrenToAcceptWriteByKey.toString());
        // remove from the set of caches that must respond to this cache
        childrenToAcceptWriteByKey.get(acceptedWriteMsg.getKey()).remove(getSender());

        System.out.println("[database] after removal, childrenToAcceptWriteByKey: " + childrenToAcceptWriteByKey.toString());

        childrenAcceptedWriteByKey.putIfAbsent(acceptedWriteMsg.getKey(), new HashSet<ActorRef>());
        childrenAcceptedWriteByKey.get(acceptedWriteMsg.getKey()).add(getSender());

        // check every time if all caches have responded for that specific key
        if (childrenToAcceptWriteByKey.get(acceptedWriteMsg.getKey()).isEmpty()) {
            log.info("[DATABASE " + id + "] All caches have responded (accepted write) for key: {}", acceptedWriteMsg.getKey());

            // here is a good place to add data to the database
            data.put(acceptedWriteMsg.getKey(), acceptedWriteMsg.getValue());
            log.info("[DATABASE " + id + "] Added data to database: key:{}, value:{}", acceptedWriteMsg.getKey(), acceptedWriteMsg.getValue());

            // send an apply write to all connected caches

            //print caches
            for (ActorRef cache : L1_caches) {
                log.info("[DATABASE " + id + "] L1 cache, onAcceptedWriteMsg: " + cache.path().name());
            }

            sendApplyWrite(acceptedWriteMsg, L1_caches);
            childrenToConfirmWriteByKey.put(acceptedWriteMsg.getKey(), L1_caches);
            log.info("[DATABASE " + id + "] Sending apply write to L1 caches");

            // send also to all L2 caches that are connected directly with the db (due to previous crashes)
            if (!L2_caches.isEmpty()) {
                sendApplyWrite(acceptedWriteMsg, L2_caches);

                Set<ActorRef> newSet = childrenToConfirmWriteByKey.get(acceptedWriteMsg.getKey());
                newSet.addAll(L2_caches);
                childrenToConfirmWriteByKey.put(acceptedWriteMsg.getKey(), newSet);

                log.info("[DATABASE " + id + "] Sending apply write ALSO to L2 caches");
            } else {
                log.info("[DATABASE " + id + "] No L2 caches connected directly with the database to send apply write");
            }

        }

    }

    public void onConfirmedWriteMsg(ConfirmedWriteMsg confirmedWriteMsg) {
        log.info("[DATABASE " + id + "] Received confirmed write msg; key:{}, value:{} from {}", confirmedWriteMsg.getKey(), confirmedWriteMsg.getValue(), getSender().path().name());

        System.out.println("[database] before removal , childrenToConfirmWriteByKey: " + childrenToConfirmWriteByKey.toString());
        // remove from the set of caches that must respond to this cache
        childrenToConfirmWriteByKey.get(confirmedWriteMsg.getKey()).remove(getSender());

        System.out.println("[database] after removal, childrenToConfirmWriteByKey: " + childrenToConfirmWriteByKey.toString());

        childrenConfirmedWriteByKey.putIfAbsent(confirmedWriteMsg.getKey(), new HashSet<ActorRef>());
        childrenConfirmedWriteByKey.get(confirmedWriteMsg.getKey()).add(getSender());

        involvedCachesCritWrites.put(confirmedWriteMsg.getKey(), childrenConfirmedWriteByKey.get(confirmedWriteMsg.getKey()));

        // check every time if all caches have responded for that specific key
        if (childrenToConfirmWriteByKey.get(confirmedWriteMsg.getKey()).isEmpty()) {
            log.info("[DATABASE " + id + "] All caches have responded (confirmed write) for key: {}", confirmedWriteMsg.getKey());

            // get the value of the key from ongoingCritWrites
            CriticalWriteRequestMsg criticalWriteRequestMsg = ongoingCritWrites.get(confirmedWriteMsg.getKey());

            // remove the key from ongoingCritWrites
            ongoingCritWrites.remove(confirmedWriteMsg.getKey());

            // print ongoingCritWrites
            System.out.println("[database] ongoingCritWrites, after removal: " + ongoingCritWrites.toString());

            Set<ActorRef> involvedCaches = involvedCachesCritWrites.remove(confirmedWriteMsg.getKey());

            CriticalWriteResponseMsg criticalWriteResponseMsg = new CriticalWriteResponseMsg(confirmedWriteMsg.getKey(), confirmedWriteMsg.getValue(), criticalWriteRequestMsg.getPath(), criticalWriteRequestMsg.getRequestId(), false);
            criticalWriteResponseMsg.setUpdatedCaches(involvedCaches);
            getSender().tell(criticalWriteResponseMsg, getSelf());
            log.info("[DATABASE " + id + "] Sending write response to cache " + getSender().path().name());

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
