package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.concurrent.duration.*;


import java.util.*;
import java.util.concurrent.TimeUnit;

import it.unitn.ds1.Message.*;

public class Client extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private int id;

    private boolean isResponseReceived = true;

    private HashSet<ActorRef> L2_caches = new HashSet<>();

    private ActorRef parent;
    private boolean isConnectedToParent = true;

    private HashMap<String, Integer> timeouts = new HashMap<>();

    private Random rnd = new Random();
    private String classString = String.valueOf(getClass());

    // operations done by the client: read, write, crit_read, crit_write
    public class ClientOperation {
        private final String operation;
        private final int key;
        private int value;
        private boolean finished;
        //private boolean success; //maybe not needed
        private final long startTime = System.currentTimeMillis();
        private long endTime;
        private long firstRequestId; // equal to the id of the first request sent for this operation

        public ClientOperation(String operation, int key, long firstRequestId) {
            this.operation = operation;
            this.key = key;
            this.finished = false;
            this.firstRequestId = firstRequestId;
        }

        // Getters for the instance variables
        public String getOperation() {
            return operation;
        }

        public int getKey() {
            return key;
        }

        public int getValue() {
            return value;
        }

        public boolean isFinished() {
            return finished;
        }

        public long getStartTime() {
            return startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public long getFirstRequestId() {
            return firstRequestId;
        }

        public void setEndTime() {
            this.endTime = System.currentTimeMillis();
        }

        public long getDuration() {
            return endTime - startTime;
        }

        //setters for the instance variables

        public void setValue(int value) {
            this.value = value;
        }

        public void setFinished(boolean finished) {
            this.finished = finished;
        }

        public String toString() {
            return "{ Operation: " + operation + ", Key: " + key + ", Value: " + value + ", Finished: " + finished  + ", Start Time: " + startTime + ", End Time: " + endTime + ", Duration: " + getDuration() + "ms }";
        }

    }

    private List<ClientOperation> operations = new ArrayList<>();

    public Client(int id, ActorRef parent, List<TimeoutConfiguration> timeouts, HashSet<ActorRef> l2Caches) {
        this.id = id;
        setParent(parent);
        setTimeouts(timeouts);
        setL2_caches(l2Caches);
    }

    static public Props props(int id, ActorRef parent, List<TimeoutConfiguration> timeouts, HashSet<ActorRef> l2Caches) {
        return Props.create(Client.class, () -> new Client(id, parent, timeouts, l2Caches));
    }

    //getter for id
    public int getId() {
        return this.id;
    }

    // ----------PARENT LOGIC----------

    public ActorRef getParent() {
        return this.parent;
    }

    public void setParent(ActorRef parent) {
        this.parent = parent;
    }

    // ----------L2 CACHE LOGIC----------

    public HashSet<ActorRef> getL2_caches() {
        return this.L2_caches;
    }

    public void setL2_caches(HashSet<ActorRef> l2_caches) {
        this.L2_caches = l2_caches;
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

    //client could receive only 1 timeout message at a time (per type maybe)
    public void startTimeout(String type, String connectionDestination) {
        log.info("[CLIENT " + id + "] Starting timeout for " + type + " operation, " + getTimeout(type) + " seconds");
        getContext().system().scheduler().scheduleOnce(
            Duration.create(getTimeout(type), TimeUnit.SECONDS),
            getSelf(),
            new TimeoutMsg(type, connectionDestination), // the message to send
            getContext().system().dispatcher(),
            getSelf()
        );

        //store current active timeout
        //activeTimeouts.put(type, true);

    }

    //data structure to store current active timeouts, ex: <"connection": true>
    //private HashMap<String, Boolean> activeTimeouts = new HashMap<>();

    // one-element list timeouts_to_skip
    // values could be: "read", "write", "crit_read", "crit_write"
    // "connection" is not considered since the "connection" operation is carried out by only one cache
    // therefore there is no need to wait more time due to an upper level timeout
    private List<String> timeouts_to_skip = new ArrayList<>();

    private boolean isResponseReceived() {
        return this.isResponseReceived;
    } //maybe not needed

    private void sendRequest() {
        this.isResponseReceived = false;
    } //maybe not needed

    private void receiveResponse() {
        this.isResponseReceived = true;
    } //maybe not needed

    public void addDelayInSeconds(int seconds) {
        try {
            log.info("[CLIENT " + id + "] Adding delay of " + seconds + " seconds");
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            log.error("[CLIENT " + id + "] Error while adding delay");
            e.printStackTrace();
        }
    }

    public void retryOperation(){
        if (operations.size() > 0){
            // get last operation on the list
            ClientOperation lastOp = operations.get(operations.size() - 1);
            if(lastOp.isFinished()){
                log.info("[CLIENT " + id + "] Operation" + lastOp.getOperation() + " finished");
            }

            //int delayInSeconds = rnd.nextInt(5);
            int delayInSeconds = 15; //to be changed, dynamic

            // we must differentiate between standard sendReadRequestMsg, sendWriteRequestMsg, etc
            // since we need to re use the same requestId
            retrySendMsg(lastOp, delayInSeconds);

        } else {
            log.info("[CLIENT " + id + "] No operations to retry");
        }
    }

    /*-- Actor logic -- */

    public void preStart() {
        log.info("[CLIENT " + id + "] Started!");
    }

    // ----------SEND LOGIC----------
    public void sendInitMsg(){
        InitMsg msg = new InitMsg(getSelf(), "client");
        parent.tell(msg, getSelf());
    }

    public void sendReadRequestMsg(int key, int delayInSeconds){

        log.info("[CLIENT " + id + "] Started creating read request msg, to be sent to " + getParent().path().name() + " with key " + key);

        addDelayInSeconds(delayInSeconds);
        log.info("[CLIENT " + id + "] Delay of " + delayInSeconds + " seconds added");

        Stack<ActorRef> path = new Stack<>();
        path.push(getSelf());
        long requestId = System.currentTimeMillis(); //To be modified
        ReadRequestMsg msg = new ReadRequestMsg(key, path, requestId);
        log.info("[CLIENT " + id + "] Created read request msg to be sent to " + getParent().path().name() + " with key " + key + " and requestId " + msg.getRequestId());

        // assumption: client can send only 1 request at a time
        // if last operation of the client is finished or there are no operations, add new operation
        if ((operations.size() > 0 && operations.get(operations.size() - 1).isFinished()) || operations.size() == 0) {
            operations.add(new ClientOperation("read", key, requestId));
            log.info("[CLIENT " + id + "] Created new read operation");

            getParent().tell(msg, getSelf());
            //sendRequest(); //maybe not needed
            log.info("[CLIENT " + id + "] Sent read request msg! to " + getParent().path().name());

            startTimeout("read", getParent().path().name());

        } else {
            //if last operation is not finished
            log.info("[CLIENT " + id + "] Cannot create new read operation, last operation not finished");
        }

    }

    public void sendWriteRequestMsg(int key, int value, int delayInSeconds){

        log.info("[CLIENT " + id + "] Started creating write request msg, to be sent to " + getParent().path().name() + " with key " + key + " and value " + value);

        addDelayInSeconds(delayInSeconds);
        log.info("[CLIENT " + id + "] Delay of " + delayInSeconds + " seconds added");

        Stack<ActorRef> path = new Stack<>();
        path.push(getSelf());
        long requestId = System.currentTimeMillis(); // To be modified
        WriteRequestMsg msg = new WriteRequestMsg(key, value, path, requestId);
        log.info("[CLIENT " + id + "] Created write request msg to be sent to " + getParent().path().name() + " with key " + key + " and value " + value);

        //assumption: client can send only 1 request at a time
        //if last operation of the client is finished or there are no operations, add new operation
        if ((operations.size() > 0 && operations.get(operations.size() - 1).isFinished()) || operations.size() == 0) {
            ClientOperation writeOp = new ClientOperation("write", key, requestId);
            writeOp.setValue(value); //since write operation has value
            operations.add(writeOp);
            log.info("[CLIENT " + id + "] Created new write operation");

            getParent().tell(msg, getSelf());
            //sendRequest(); //maybe not needed
            log.info("[CLIENT " + id + "] Sent write request msg! to " + getParent().path().name());

            startTimeout("write", getParent().path().name());

        } else {
            //if last operation is not finished
            log.info("[CLIENT " + id + "] Cannot create new write operation, last operation not finished");
        }

    }

    public void sendCriticalReadRequestMsg(int key, int delayInSeconds){

        log.info("[CLIENT " + id + "] Started creating critical read request msg, to be sent to " + getParent().path().name() + " with key " + key);

        addDelayInSeconds(delayInSeconds);
        log.info("[CLIENT " + id + "] Delay of " + delayInSeconds + " seconds added");

        Stack<ActorRef> path = new Stack<>();
        path.push(getSelf());
        long requestId = System.currentTimeMillis(); //To be modified
        CriticalReadRequestMsg msg = new CriticalReadRequestMsg(key, path, requestId);
        log.info("[CLIENT " + id + "] Created critical read request msg to be sent to " + getParent().path().name() + " with key " + key + " and requestId " + msg.getRequestId());

        // assumption: client can send only 1 request at a time
        // if last operation of the client is finished or there are no operations, add new operation
        if ((operations.size() > 0 && operations.get(operations.size() - 1).isFinished()) || operations.size() == 0) {
            operations.add(new ClientOperation("crit_read", key, requestId));
            log.info("[CLIENT " + id + "] Created new critical read operation");

            getParent().tell(msg, getSelf());
            log.info("[CLIENT " + id + "] Sent critical read request msg! to " + getParent().path().name());

            startTimeout("crit_read", getParent().path().name());

        } else {
            //if last operation is not finished
            log.info("[CLIENT " + id + "] Cannot create new read operation, last operation not finished");
        }

    }

    public void sendCriticalWriteRequestMsg(int key, int value, int delayInSeconds){

        log.info("[CLIENT " + id + "] Started creating critical write request msg, to be sent to " + getParent().path().name() + " with key " + key + " and value " + value);

        addDelayInSeconds(delayInSeconds);
        log.info("[CLIENT " + id + "] Delay of " + delayInSeconds + " seconds added");

        Stack<ActorRef> path = new Stack<>();
        path.push(getSelf());
        long requestId = System.currentTimeMillis(); // To be modified
        CriticalWriteRequestMsg msg = new CriticalWriteRequestMsg(key, value, path, requestId);
        log.info("[CLIENT " + id + "] Created critical write request msg to be sent to " + getParent().path().name() + " with key " + key + " and value " + value);

        //assumption: client can send only 1 request at a time
        //if last operation of the client is finished or there are no operations, add new operation
        if ((operations.size() > 0 && operations.get(operations.size() - 1).isFinished()) || operations.size() == 0) {
            ClientOperation critWriteOp = new ClientOperation("crit_write", key, requestId);
            critWriteOp.setValue(value); //since write operation has value
            operations.add(critWriteOp);
            log.info("[CLIENT " + id + "] Created new critical write operation");

            getParent().tell(msg, getSelf());
            //sendRequest(); //maybe not needed
            log.info("[CLIENT " + id + "] Sent critical write request msg! to " + getParent().path().name());

            startTimeout("crit_write", getParent().path().name());

        } else {
            //if last operation is not finished
            log.info("[CLIENT " + id + "] Cannot create new write operation, last operation not finished");
        }

    }





    public void retrySendMsg(ClientOperation operation, int delayInSeconds){

        System.out.println("Operations size: " + operations.size()); //debug, should be at least 1

        Stack<ActorRef> path = new Stack<>();
        path.push(getSelf());
        long requestId = operation.getFirstRequestId(); // Retrieving the first request id of the operation

        switch(operation.getOperation()) {
            case "read":
                log.info("[CLIENT " + id + "] Retrying sending read request msg, to be sent to " + getParent().path().name() + " with key " + operation.getKey());

                addDelayInSeconds(delayInSeconds);
                log.info("[CLIENT " + id + "] Delay of " + delayInSeconds + " seconds added");

                ReadRequestMsg readRequestMsg = new ReadRequestMsg(operation.getKey(), path, requestId);
                log.info("[CLIENT " + id + "] Created read request msg to be sent to " + getParent().path().name() + " with key " + readRequestMsg.getKey() + " and requestId " + readRequestMsg.getRequestId());

                getParent().tell(readRequestMsg, getSelf());
                log.info("[CLIENT " + id + "] Sent read request msg! to " + getParent().path().name());

                startTimeout("read", getParent().path().name());
                log.info("[CLIENT " + id + "] Started timeout for read request msg! to " + getParent().path().name());
                break;
            case "write":
                log.info("[CLIENT " + id + "] Started creating write request msg, to be sent to " + getParent().path().name() + " with key " + operation.getKey() + " and value " + operation.getValue());

                addDelayInSeconds(delayInSeconds);
                log.info("[CLIENT " + id + "] Delay of " + delayInSeconds + " seconds added");

                WriteRequestMsg writeRequestMsg = new WriteRequestMsg(operation.getKey(), operation.getValue(), path, requestId);
                log.info("[CLIENT " + id + "] Created write request msg to be sent to " + getParent().path().name() + " with key " + writeRequestMsg.getKey() + " and value " + writeRequestMsg.getValue());

                getParent().tell(writeRequestMsg, getSelf());
                log.info("[CLIENT " + id + "] Sent write request msg! to " + getParent().path().name());

                startTimeout("write", getParent().path().name());
                log.info("[CLIENT " + id + "] Started timeout for write request msg! to " + getParent().path().name());
                break;
            case "crit_read":
                log.info("[CLIENT " + id + "] Retrying sending critical read request msg, to be sent to " + getParent().path().name() + " with key " + operation.getKey());

                addDelayInSeconds(delayInSeconds);
                log.info("[CLIENT " + id + "] Delay of " + delayInSeconds + " seconds added");

                CriticalReadRequestMsg criticalReadRequestMsg = new CriticalReadRequestMsg(operation.getKey(), path, requestId);
                log.info("[CLIENT " + id + "] Created critical read request msg to be sent to " + getParent().path().name() + " with key " + criticalReadRequestMsg.getKey() + " and requestId " + criticalReadRequestMsg.getRequestId());

                getParent().tell(criticalReadRequestMsg, getSelf());
                log.info("[CLIENT " + id + "] Sent critical read request msg! to " + getParent().path().name());

                startTimeout("crit_read", getParent().path().name());
                log.info("[CLIENT " + id + "] Started timeout for critical read request msg! to " + getParent().path().name());
                break;
            case "crit_write":
                log.info("[CLIENT " + id + "] Started creating write request msg, to be sent to " + getParent().path().name() + " with key " + operation.getKey() + " and value " + operation.getValue());

                addDelayInSeconds(delayInSeconds);
                log.info("[CLIENT " + id + "] Delay of " + delayInSeconds + " seconds added");

                CriticalWriteRequestMsg criticalWriteRequestMsg = new CriticalWriteRequestMsg(operation.getKey(), operation.getValue(), path, requestId);
                log.info("[CLIENT " + id + "] Created critical write request msg to be sent to " + getParent().path().name() + " with key " + criticalWriteRequestMsg.getKey() + " and value " + criticalWriteRequestMsg.getValue());

                getParent().tell(criticalWriteRequestMsg, getSelf());
                log.info("[CLIENT " + id + "] Sent critical write request msg! to " + getParent().path().name());

                startTimeout("crit_write", getParent().path().name());
                log.info("[CLIENT " + id + "] Started timeout for critical write request msg! to " + getParent().path().name());
                break;
        }

    }

    // ----------RECEIVE LOGIC----------

    // Here we define the mapping between the received message types and the database methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartInitMsg.class, this::onStartInitMsg)

                .match(StartReadRequestMsg.class, this::onStartReadRequestMsg)
                .match(StartWriteMsg.class, this::onStartWriteMsg)
                .match(StartCriticalReadRequestMsg.class, this::onStartCriticalReadRequestMsg)
                .match(StartCriticalWriteRequestMsg.class, this::onStartCriticalWriteRequestMsg)

                .match(ReadResponseMsg.class, this::onReadResponseMsg)
                .match(WriteResponseMsg.class, this::onWriteResponseMsg)
                .match(CriticalReadResponseMsg.class, this::onCriticalReadResponseMsg)
                .match(CriticalWriteResponseMsg.class, this::onCriticalWriteResponseMsg)

                .match(TimeoutMsg.class, this::onTimeoutMsg)
                .match(TimeoutElapsedMsg.class, this::onTimeoutElapsedMsg)
                .match(ResponseConnectionMsg.class, this::onResponseConnectionMsg)
                .match(InfoMsg.class, this::onInfoMsg)

                .matchAny(o -> log.debug("[CLIENT " + id + "] received unknown message from " +
                        getSender().path().name() + ": " + o))
                .build();
    }

    private void onInfoMsg (InfoMsg msg){
        log.info("[CLIENT {}] Parent: ", getId(), getParent().path().name());
    }

    private void onStartInitMsg(Message.StartInitMsg msg) {
        //CustomPrint.print(classString,"", String.valueOf(this.id), " Received initialization msg!");
        log.info("[CLIENT " + id + "] Received initialization msg!");
        sendInitMsg();
    }

    private void onTimeoutMsg(TimeoutMsg msg) {

        log.info("[CLIENT " + id + "] Received timeout msg of type " + msg.getType() + " with destination " + msg.getConnectionDestination());

        // check timeout msg type
        System.out.println("msg type " + msg.getType());
        if(Objects.equals(msg.getType(), "read")
            || Objects.equals(msg.getType(), "write")
            || Objects.equals(msg.getType(), "crit_read")
            || Objects.equals(msg.getType(), "crit_write")){

            //System.out.println("inside first if");
            //check if operation is finished in the meantime
            System.out.println("operations size: " + operations.size());
            if (operations.size() > 0 && operations.get(operations.size() - 1).isFinished()) {
                log.info("[CLIENT " + id + "] Operation " + operations.get(operations.size() - 1).getOperation() + " already finished");
                log.info("[CLIENT " + id + "] Ignoring timeout msg");
                return;
            }

            // check if it is a timeout to be skipped
            // use case: a L2 cache tells the client that it needs more time to fulfill client's request
            System.out.println("timeouts to skip size: " + timeouts_to_skip.size());
            if(timeouts_to_skip.size() > 0){
                System.out.println("inside second if");
                String current_op = operations.get(operations.size() - 1).getOperation();
                if(timeouts_to_skip.get(0).equals(current_op)){
                    timeouts_to_skip.remove(0);
                    log.info("[CLIENT " + id + "] Skipping timeout msg, since parent is still processing the request and needs more time");
                    return;
                }
            }
        }

        if(msg.getType() == "connection") {
            //check if client is connected to parent in the meantime
            if(isConnectedToParent){
                log.info("[CLIENT " + id + "] Ignoring timeout msg");
                return;
            }
        }

        // the following logic is true for every timeout msg type: read, write, crit_read, crit_write, connection
        // we lost connection to the parent, so we need to find a new one
        this.isConnectedToParent = false;

        log.info("[CLIENT " + id + "] Trying to connect to another L2 cache");
        Set<ActorRef> caches = getL2_caches();
        ActorRef[] tmpArray = caches.toArray(new ActorRef[caches.size()]);
        ActorRef cache = null;

        // possible improvement: treat the set as a circular array
        while(cache == this.parent || cache == null) {
            // generate a random number
            Random rnd = new Random();

            // this will generate a random number between 0 and Set.size - 1
            int rndNumber = rnd.nextInt(caches.size());
            cache = tmpArray[rndNumber];
        }

        setParent(cache);
        log.info("[CLIENT " + id + "] New designated parent: " + getParent().path().name());

        getParent().tell(new RequestConnectionMsg(), getSelf());
        log.info("[CLIENT " + id + "] Sent request connection msg to " + getParent().path().name());

        startTimeout("connection", getParent().path().name());
        log.info("[CLIENT " + id + "] Started connection timeout");
    }

    // use case: client receives a response from a L2 cache
    // L2 cache tells the client that it needs more time to fulfill client's request
    private void onTimeoutElapsedMsg (TimeoutElapsedMsg msg){
        log.info("[CLIENT " + id + "] Received timeout elapsed msg from {}!", getSender().path().name());

        // check what is the current operation ongoing (read, write, crit_read, crit_write)
        // assumption: only one operation can be ongoing at a time per client
        String current_op = operations.get(operations.size() - 1).getOperation();
        //System.out.println("current op: " + current_op);
        //System.out.println("msg type: " + msg.getType());
        if(current_op != msg.getType()){
            // should never happen with the current assumptions
            log.info("[CLIENT " + id + "] Received timeout elapsed msg of type " + msg.getType() + " but current operation is " + current_op);
            return;
        }

        // add to timeout_to_skip the operation type of the msg received
        // that operation must match the current operation ongoing
        // timeout_to_skip is a one-element list since only one operation can be ongoing at a time per client
        // assumption: you get only one timeout elapsed msg per time
        //System.out.println("timeouts to skip size: " + timeouts_to_skip.size());
        if(timeouts_to_skip.size() == 0){
            //System.out.println("inside if of timeout elapsed msg");
            timeouts_to_skip.add(msg.getType());
            log.info("[CLIENT " + id + "] Added " + msg.getType() + "timeout to timeouts_to_skip");
        }

        // start a new timeout with the same type as the current operation ongoing
        startTimeout(msg.getType(), getSender().path().name());
        log.info("[CLIENT " + id + "] Will wait for another timeout msg of type " + msg.getType() + " from " + getSender().path().name());
        log.info("[CLIENT " + id + "] Started " + msg.getType() + " timeout");

    }

    public void onResponseConnectionMsg(ResponseConnectionMsg msg){

        // this is the case when a L2 cache crashes and the client tries to connect to another L2 cache

        //receiveResponse();
        log.info("[CLIENT " + id + "] Received response connection msg from " + getSender().path().name());

        if(msg.getResponse().equals("ACCEPTED")){
            log.info("[CLIENT " + id + "] Connection established with " + getSender().path().name());
            this.isConnectedToParent = true;

            /*
            //stop connection timeout
            if(activeTimeouts.get("connection") == true){
                activeTimeouts.put("connection", false);
                log.info("[CLIENT " + id + "] Stopped connection timeout");
            }
             */

            //retrying last operation
            retryOperation();
            log.info("[CLIENT " + id + "] Retrying operation");
        }

    }

    public void onStartReadRequestMsg(Message.StartReadRequestMsg msg) {
        log.info("[CLIENT " + id + "] Received start read request msg!");
        int delayInSeconds = 60;
        sendReadRequestMsg(msg.key, delayInSeconds);
    }

    // ----------WRITE MESSAGES LOGIC----------
    private void onStartWriteMsg(StartWriteMsg msg) { //TODO: change name according to the read
        log.info("[CLIENT " + id + "] Received write msg request!");
        int delayInSeconds = 45;
        sendWriteRequestMsg(msg.key, msg.value, delayInSeconds);
    }

    private void onStartCriticalReadRequestMsg(StartCriticalReadRequestMsg msg){
        log.info("[CLIENT " + id + "] Received critical read msg request!");
        int delayInSeconds = 60;
        sendCriticalReadRequestMsg(msg.getKey(), delayInSeconds);
    }

    private void onStartCriticalWriteRequestMsg(StartCriticalWriteRequestMsg msg){
        log.info("[CLIENT " + id + "] Received critical write msg request!");
        int delayInSeconds = 10;
        sendCriticalWriteRequestMsg(msg.getKey(), msg.getValue(), delayInSeconds);
    }

    public void onReadResponseMsg(ReadResponseMsg msg){
        //receiveResponse(); //maybe not needed
        log.info("[CLIENT " + id + "] Received read response from " + getSender().path().name() + " with value " + msg.getValue() + " for key " + msg.getKey());
        operations.get(operations.size() - 1).setValue(msg.getValue());
        operations.get(operations.size() - 1).setFinished(true);
        operations.get(operations.size() - 1).setEndTime();
        log.info("[CLIENT " + id + "] Operation " + operations.get(operations.size() - 1).getOperation() + " finished");
        log.info("[CLIENT " + id + "] Operations list: " + operations.toString());
    }

    public void onCriticalReadResponseMsg(CriticalReadResponseMsg msg){
        //receiveResponse(); //maybe not needed
        log.info("[CLIENT " + id + "] Received CritRead response from " + getSender().path().name() + " with value " + msg.getValue() + " for key " + msg.getKey());
        operations.get(operations.size() - 1).setValue(msg.getValue());
        operations.get(operations.size() - 1).setFinished(true);
        operations.get(operations.size() - 1).setEndTime();
        log.info("[CLIENT " + id + "] Operation " + operations.get(operations.size() - 1).getOperation() + " finished");
        log.info("[CLIENT " + id + "] Operations list: " + operations.toString());
    }

    private void onWriteResponseMsg(WriteResponseMsg msg) {
        log.info("[CLIENT " + id + "] Received write response msg, with value " + msg.getValue() + " for key " + msg.getKey() + " from " + getSender().path().name());

        operations.get(operations.size() - 1).setValue(msg.getValue());
        operations.get(operations.size() - 1).setFinished(true);
        operations.get(operations.size() - 1).setEndTime();
        log.info("[CLIENT " + id + "] Operation " + operations.get(operations.size() - 1).getOperation() + " finished");
        log.info("[CLIENT " + id + "] Operations list: " + operations.toString());
        // when interacting with the same cache (l2 cache)
        // the client is guaranteed not to read a value older than the last write
    }

    private void onCriticalWriteResponseMsg(CriticalWriteResponseMsg msg) {
        log.info("[CLIENT " + id + "] Received critical write response msg, with value " + msg.getValue() + " for key " + msg.getKey() + " from " + getSender().path().name());

        // print updated caches
        log.info("[CLIENT " + id + "] Updated caches: " + msg.printUpdatedCaches());

        operations.get(operations.size() - 1).setValue(msg.getValue());
        operations.get(operations.size() - 1).setFinished(true);
        operations.get(operations.size() - 1).setEndTime();
        log.info("[CLIENT " + id + "] Operation " + operations.get(operations.size() - 1).getOperation() + " finished");
        log.info("[CLIENT " + id + "] Operations list: " + operations.toString());

    }
}
