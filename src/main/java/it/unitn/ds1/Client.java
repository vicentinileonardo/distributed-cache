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

    // operations done by the client
    public class ClientOperation {
        private final String operation;
        private final int key;
        private int value;
        private boolean finished;
        //private boolean success; //maybe not needed
        private final long startTime = System.currentTimeMillis();
        private long endTime;

        public ClientOperation(String operation, int key) {
            this.operation = operation;
            this.key = key;
            this.finished = false;
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
            return "Operation: " + operation + ", Key: " + key + ", Value: " + value + ", Finished: " + finished  + ", Start Time: " + startTime + ", End Time: " + endTime + ", Duration: " + getDuration() + "ms";
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
            //get last operation on the list
            ClientOperation lastOp = operations.get(operations.size() - 1);
            if(lastOp.isFinished()){
                log.info("[CLIENT " + id + "] Operation" + lastOp.getOperation() + " finished");
            }
            String operation = lastOp.getOperation();
            //int delayInSeconds = rnd.nextInt(5);
            int delayInSeconds = 15;
            switch(operation){
                case "read":
                    sendReadRequestMsg(lastOp.getKey(), delayInSeconds);
                    log.info("[CLIENT " + id + "] Retrying read operation with key " + lastOp.getKey() + " with delay of " + delayInSeconds + " seconds");
                    break;


                //TODO: add other operations
                default:
                    log.info("[CLIENT " + id + "] Operation not recognized");
                    break;
            }
        }
    }

    /*-- Actor logic -- */

    public void preStart() {
        //CustomPrint.print(classString,"", String.valueOf(this.id), " Started!");
        log.info("[CLIENT " + id + "] Started!");
    }

    // ----------SEND LOGIC----------
    public void sendInitMsg(){
        InitMsg msg = new InitMsg(getSelf(), "client");
        parent.tell(msg, getSelf());
    }

    public void sendReadRequestMsg(int key, int delayInSeconds){

        log.info("[CLIENT " + id + "] Creating read request msg, to be sent to " + getParent().path().name() + " with key " + key);

        addDelayInSeconds(delayInSeconds);

        Stack<ActorRef> path = new Stack<>();
        path.push(getSelf());
        long requestId = System.currentTimeMillis(); //To be modified
        ReadRequestMsg msg = new ReadRequestMsg(key, path, requestId);
        log.info("[CLIENT " + id + "] Created read request msg to be sent to " + getParent().path().name() + " with key " + key + " and requestId " + msg.getRequestId());

        //assumption: client can send only 1 request at a time
        //if last operation of the client is finished or there are no operations, add new operation
        if ((operations.size() > 0 && operations.get(operations.size() - 1).isFinished()) || operations.size() == 0) {
            operations.add(new ClientOperation("read", key));
            log.info("[CLIENT " + id + "] Created new read operation");
        }

        getParent().tell(msg, getSelf());
        //sendRequest(); //maybe not needed
        log.info("[CLIENT " + id + "] Sent read request msg! to " + getParent().path().name());

        startTimeout("read", getParent().path().name());
    }

    public void onReadResponseMsg(Message.ReadResponseMsg msg){
        //receiveResponse(); //maybe not needed
        log.info("[CLIENT " + id + "] Received read response from " + getSender().path().name() + " with value " + msg.getValue() + " for key " + msg.getKey());
        operations.get(operations.size() - 1).setValue(msg.getValue());
        operations.get(operations.size() - 1).setFinished(true);
        operations.get(operations.size() - 1).setEndTime();
        log.info("[CLIENT " + id + "] Operation " + operations.get(operations.size() - 1).getOperation() + " finished");
        log.info("[CLIENT " + id + "] Operations list: " + operations.toString());
    }

    public void sendWriteMsg(int key, int value){
        Stack<ActorRef> path = new Stack<>();
        path.push(getSelf());
        Message.WriteMsg msg = new Message.WriteMsg(key, value, path);
        parent.tell(msg, getSelf());
        CustomPrint.print(classString,"", String.valueOf(this.id), " Sent write msg!");
    }

    // ----------RECEIVE LOGIC----------

    // Here we define the mapping between the received message types and the database methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartInitMsg.class, this::onStartInitMsg)
                .match(StartReadRequestMsg.class, this::onStartReadRequestMsg)
                .match(StartWriteMsg.class, this::onStartWriteMsg)
                .match(ReadResponseMsg.class, this::onReadResponseMsg)
                .match(WriteConfirmationMsg.class, this::onWriteConfirmationMsg)
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

        //check timeout msg type
        if(msg.getType() == "read" || msg.getType() == "write") {
            //check if operation is finished in the meantime
            if (operations.size() > 0 && operations.get(operations.size() - 1).isFinished()) {
                log.info("[CLIENT " + id + "] Operation " + operations.get(operations.size() - 1).getOperation() + " already finished");
                log.info("[CLIENT " + id + "] Ignoring timeout msg");
                return;
            }
        }

        if(msg.getType() == "connection") {
            //check if client is connected to parent in the meantime
            if(isConnectedToParent){
                log.info("[CLIENT " + id + "] Ignoring timeout msg");
                return;
            }
        }


        //the following is true for every timeout msg type: read, write, connection

        this.isConnectedToParent = false;

        log.info("[CLIENT " + id + "] Trying to connect to another L2 cache");
        Set<ActorRef> caches = getL2_caches();
        ActorRef[] tmpArray = caches.toArray(new ActorRef[caches.size()]);
        ActorRef cache = null;

        //possible improvement: treat the set as a circular array
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

    //not used for now
    private void onTimeoutElapsedMsg (TimeoutElapsedMsg msg){
        receiveResponse();
        log.info("[CLIENT " + id + "] Received timeout elapsed msg from {}!", getSender().path().name());
        log.info("[CLIENT " + id + "] Waiting for response from " + getParent().path().name());
    }

    public void onResponseConnectionMsg(ResponseConnectionMsg msg){
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
        //CustomPrint.print(classString,"", String.valueOf(this.id), " Received start read request msg!");
        log.info("[CLIENT " + id + "] Received start read request msg!");
        int delayInSeconds = 20;
        sendReadRequestMsg(msg.key, delayInSeconds );
    }

    // ----------WRITE MESSAGES LOGIC----------
    private void onStartWriteMsg(Message.StartWriteMsg msg) {
        CustomPrint.print(classString,"", String.valueOf(this.id), " Received write msg!");
        sendWriteMsg(msg.key, msg.value);
    }

    private void onWriteConfirmationMsg(Message.WriteConfirmationMsg msg) {
        CustomPrint.print(classString,"", String.valueOf(this.id), " Received write confirmation msg!");
    }
}
