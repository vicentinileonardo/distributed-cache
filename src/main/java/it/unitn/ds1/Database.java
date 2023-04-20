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

    //DEBUG ONLY: assumption that the database is always up
    private boolean crashed = false;
    private boolean responded = false;
    private Map<Integer, Integer> data = new HashMap<>();

    // since the db and different type of caches have different interactions
    // it is better to have a different set for each type of cache
    private Set<ActorRef> L2_caches = new HashSet<>();
    private Set<ActorRef> L1_caches = new HashSet<>();

    private HashSet<ActorRef> writeConfirmed = new HashSet<>();
    private Stack<ActorRef> idleResponse;
    private Map.Entry<Integer, Integer> idleData = new AbstractMap.SimpleEntry<>(null, null);;

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

    // ----------DATA LOGIC----------
    private void addData(int key, int value){
        this.data.put(key, value);
    }

    private void addData(Map<Integer, Integer> data){
        this.data.putAll(data);
    }

    private boolean isDataPresent(int key){
        return this.data.containsKey(key);
    }

    private void removeData(int key){
        this.data.remove(key);
    }

    private Integer getData(int key){
        return this.data.get(key);
    }

    private Map<Integer, Integer> getData(){
        return this.data;
    }

    private void clearData(){
        this.data.clear();
    }

    private void setIdleData(int key, int value){
        this.idleData = new AbstractMap.SimpleEntry<>(key, value);
    }

    private Map.Entry<Integer, Integer> getIdleData(){
        return this.idleData;
    }

    private Integer getIdleKey(){
        return this.idleData.getKey();
    }

    // ----------L2 CACHES LOGIC----------

    private Set<ActorRef> getL2_caches() {
        return this.L2_caches;
    }

    private void setL2_caches(Set<ActorRef> l2_caches) {
        this.L2_caches = l2_caches;
    }

    private void addL2_cache(ActorRef l2_cache) {
        this.L1_caches.add(l2_cache);
    }

    private boolean isL2Empty(){
        return this.L2_caches.isEmpty();
    }

    private void removeL2_cache(ActorRef l2_cache) {
        this.L1_caches.remove(l2_cache);
    }

    private boolean getL2_cache(ActorRef l2_cache) {
        return this.L2_caches.contains(l2_cache);
    }

    // ----------L1 CACHES LOGIC----------

    private Set<ActorRef> getL1_caches() {
        return this.L1_caches;
    }

    private void setL1_caches(Set<ActorRef> l1_caches) {
        this.L1_caches = l1_caches;
    }

    private void addL1_cache(ActorRef l1_cache) {
        this.L1_caches.add(l1_cache);
    }

    private void removeL1_cache(ActorRef l1_cache) {
        this.L1_caches.remove(l1_cache);
    }

    private boolean getL1_cache(ActorRef l1_cache) {
        return this.L1_caches.contains(l1_cache);
    }

    private boolean isL1Empty(){
        return this.L1_caches.isEmpty();
    }


    // ----------TIMEOUT LOGIC----------

    private void setTimeouts(List<TimeoutConfiguration> timeouts){
        for (TimeoutConfiguration timeout: timeouts){
            setTimeout(timeout.getType(), timeout.getValue());
        }
    }

    private HashMap<String, Integer> getTimeouts(){
        return this.timeouts;
    }

    private int getTimeout(String type){
        return this.timeouts.get(type);
    }

    private void setTimeout(String type, int value){
        this.timeouts.put(type, value);
    }

    private void startTimeout(String type){
        getContext().getSystem().getScheduler().scheduleOnce(
                Duration.create(getTimeout(type)*1000L, TimeUnit.MILLISECONDS),
                getSelf(),
                new TimeoutMsg(), // the message to send
                getContext().system().dispatcher(),
                getSelf()
        );
    }

    // ----------RESPONSE LOGIC----------
    public Stack<ActorRef> getIdleResponse() {
        return this.idleResponse;
    }

    public ActorRef getIdleDestination() {
        return this.idleResponse.pop();
    }

    public void setIdleResponse(Stack<ActorRef> idleResponse) {
        this.idleResponse = idleResponse;
    }

    private boolean hasResponded(){
        return this.responded;
    }

    private void sendRequest(){
        this.responded = false;
    }
    private void receivedResponse(){
        this.responded = true;
    }

    private void addRequest(ActorRef actor){
        this.writeConfirmed.add(actor);
    }

    private boolean hasConfirmed(ActorRef actor){
        return this.writeConfirmed.contains(actor);
    }

    private void confirmedRequest(ActorRef actor){
        if(hasConfirmed(actor)){
            this.writeConfirmed.remove(actor);
        }
    }

    private boolean haveAllConfirmed(){
        return this.writeConfirmed.isEmpty();
    }

    /*-- Actor logic -- */

    @Override
    public void preStart() {

        populateDatabase();
        log.info("[DATABASE] Started!");
        log.info("[DATABASE] Initial data in database : ");
        for (Map.Entry<Integer, Integer> entry : getData().entrySet()) {
            log.info("[DATABASE] Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }

    private void populateDatabase() {
        for (int i = 0; i < 10; i++) {
            addData(i, rnd.nextInt(200));
        }
        log.debug("[DATABASE] Populated!");
    }

    // ----------SENDING LOGIC----------

    private void sendWriteConfirmation(WriteRequestMsg msg, Set<ActorRef> caches) {
        for (ActorRef cache : caches) {
            if (cache.equals(msg.getPath().get(msg.getPath().size() - 1))) {
                Stack<ActorRef> tmpStack = msg.getPath();
                tmpStack.pop();
                cache.tell(new Message.WriteResponseMsg(msg.getKey(), msg.getValue(), tmpStack), getSelf());
                log.debug("[DATABASE] Sending write confirmation to " + cache.path().name());
            } else {
                cache.tell(new FillMsg(msg.getKey(), msg.getValue()), getSelf());
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
                .match(WriteRequestMsg.class, this::onWriteRequestMsg)
                .match(CriticalReadRequestMsg.class, this::onCriticalReadRequestMsg)
                .match(RefillResponseMsg.class, this::onRefillResponseMsg)
                .match(WriteRequestMsg.class, this::onWriteRequestMsg)
                .match(CriticalReadRequestMsg.class, this::onCriticalReadRequestMsg)
                .match(RequestUpdatedDataMsg.class, this::onRequestUpdatedDataMsg)
                .match(RequestConnectionMsg.class, this::onRequestConnectionMsg)
                .matchAny(o -> log.info("[DATABASE] Received unknown message from "+ getSender()))
                .build();
    }

    // ----------INITIALIZATION MESSAGES LOGIC----------
    private void onInitMsg(Message.InitMsg msg) throws InvalidMessageException{

        if (!Objects.equals(msg.getType(), "L1")) {
            throw new InvalidMessageException("Message to wrong destination!");
        }
        addL1_cache(msg.getId());
        // log.info("[DATABASE] Added L1 cache {} as a child!", getSender().path().name());
    }

    private void onRequestConnectionMsg(RequestConnectionMsg msg) {
        if (!msg.getType().isEmpty()){
            if (msg.getType().equals("L2")){
                addL2_cache(getSender());
            } else {
                addL1_cache(getSender());
            }
        }
    }

    // ----------READ MESSAGES LOGIC----------
    private void onReadRequestMsg(ReadRequestMsg msg) {
        log.debug("[DATABASE] Received read request for key {} from {}",
                msg.getKey(), getSender().path().name());
        int value = getData(msg.getKey());
        log.debug("[DATABASE] Read value {} for key {}", value, msg.getKey());
    }

    private void onCriticalReadRequestMsg(CriticalReadRequestMsg msg){
        log.debug("[DATABASE][CRITICAL] Received read request for key {} from {}",
                msg.getKey(), getSender().path().name());
        int value = getData(msg.getKey());
        log.debug("[DATABASE][CRITICAL] Read value {} for key {}", value, msg.getKey());
        Stack<ActorRef> tmpStack = msg.getPath();
        ActorRef destination = tmpStack.pop();
        destination.tell(new CriticalReadResponseMsg(msg.getKey(), value, tmpStack), getSelf());
    public void onReadRequestMsg(ReadRequestMsg msg) {
        log.debug("[DATABASE] Received read request for key {} from {}",
                msg.getKey(), getSender().path().name());
        int value = getData(msg.getKey());
        log.debug("[DATABASE] Read value {} for key {}", value, msg.getKey());
    }

    public void onCriticalReadRequestMsg(CriticalReadRequestMsg msg){
        log.debug("[DATABASE][CRITICAL] Received read request for key {} from {}",
                msg.getKey(), getSender().path().name());
        int value = getData(msg.getKey());
        log.debug("[DATABASE][CRITICAL] Read value {} for key {}", value, msg.getKey());
        ActorRef destination = msg.getDestination();
        destination.tell(new CriticalReadResponseMsg(msg.getKey(), value, msg.getPath()), getSelf());
    }

    // ----------WRITE MESSAGES LOGIC----------
    private void onWriteRequestMsg(WriteRequestMsg msg) {
        log.debug("[DATABASE] Received write request for key {} with value {} from {}",
                msg.getKey(), msg.getValue(), getSender().path().name());
        addData(msg.getKey(), msg.getValue());
        log.debug("[DATABASE] Wrote value {} for key {}", msg.getValue(), msg.getKey());
        // notify all L1 caches
        log.info("[DATABASE] Send write confirmation to L1 caches");
        sendWriteConfirmation(msg, getL1_caches());

        // notify all L2 caches that are connected directly with the db
        if (!isL2Empty()) {
            log.info("[DATABASE] Send write confirmation to L2 caches directly connected!");
            sendWriteConfirmation(msg, getL2_caches());
        }

    }

    private void onRequestUpdatedDataMsg(RequestUpdatedDataMsg msg){
        Map<Integer, Integer> tmpData = new HashMap<>();
        for (Integer key : msg.getKeys()){
            tmpData.put(key, getData(key));
        }

        getSender().tell(new ResponseUpdatedDataMsg(tmpData), getSelf());
    }

    private void onRefillResponseMsg(RefillResponseMsg msg){
        if (!hasResponded()){
            confirmedRequest(getSender());
            if(haveAllConfirmed()){
                receivedResponse();
                // send confirmation through path
                addData(msg.getKey(), msg.getValue());
                ActorRef destination = getIdleDestination();
                destination.tell(new CriticalWriteResponseMsg(msg.getKey(), msg.getValue(), getIdleResponse()), getSelf());
            }
        }
    }

    // ----------GENERAL DATABASE MESSAGES LOGIC----------
    public void onCurrentDataMsg(CurrentDataMsg msg) {
        CustomPrint.debugPrint(classString, "","", "Current data in database :");
    private void onCurrentDataMsg(CurrentDataMsg msg) {
        CustomPrint.debugPrint(classString, "","", "Current data in database :");
        log.debug("[DATABASE] Current data in database:");
        for (Map.Entry<Integer, Integer> entry : getData().entrySet()) {
            log.debug("[DATABASE] Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }

    // DEBUG ONLY: assumption is that the database is always up
    private void onDropDatabaseMsg(DropDatabaseMsg msg) {
        log.debug("[DATABASE] Database drop request!");
        clearData();
        log.debug("[DATABASE] Dropped database!");
    }
}
