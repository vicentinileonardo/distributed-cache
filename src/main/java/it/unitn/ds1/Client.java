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

    //DEBUG ONLY: assumption that clients are always up
    private boolean crashed = false;
    private boolean responded = true;

    private HashSet<ActorRef> L2_caches = new HashSet<>();

    private ActorRef parent;

    private HashMap<String, Integer> timeouts = new HashMap<>();

    private Random rnd = new Random();
    private String classString = String.valueOf(getClass());

    // operations done by the client
    public class ClientOperation {
        private final String operation;
        private final int key;
        private int value;
        private boolean finished;
        private boolean success;
        private final long startTime = System.currentTimeMillis();
        private long endTime;

        public ClientOperation(String operation, int key) {
            this.operation = operation;
            this.key = key;
            this.finished = false;
            this.success = false;
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

        public boolean isSuccess() {
            return success;
        }

        public long getStartTime() {
            return startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
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

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String toString() {
            return "Operation: " + operation + ", Key: " + key + ", Value: " + value + ", Finished: " + finished + ", Success: " + success + ", Start Time: " + startTime + ", End Time: " + endTime + ", Duration: " + getDuration();
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

    public void startTimeout(String type) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(getTimeout(type), TimeUnit.SECONDS),
                getSelf(),
                new TimeoutMsg(), // the message to send
                getContext().system().dispatcher(),
                getSelf()
        );
    }

    private boolean hasResponded() {
        return this.responded;
    }

    private void sendRequest() {
        this.responded = false;
    }

    private void receivedResponse() {
        this.responded = true;
    }

    public void addDelayInSeconds(int seconds) {
        try {
            log.info("[CLIENT " + id + "] Adding delay of " + seconds + " seconds");
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            log.info("[CLIENT " + id + "] Error while adding delay");
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
            switch(operation){
                case "read":
                    sendReadRequestMsg(lastOp.getKey());
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
        CustomPrint.print(classString,"", String.valueOf(this.id), " Started!");
    }

    // ----------SEND LOGIC----------
    public void sendInitMsg(){
        Message.InitMsg msg = new Message.InitMsg(getSelf(), "client");
        parent.tell(msg, getSelf());
    }

    public void sendReadRequestMsg(int key){
        //log parent
        log.info("[CLIENT " + id + "] Sending (after delay) read request msg to " + getParent());
        addDelayInSeconds(20);
        Stack<ActorRef> path = new Stack<>();
        path.push(getSelf());
        Message.ReadRequestMsg msg = new Message.ReadRequestMsg(key, path);

        //if last operation is finished or there are no operations, add new operation
        if (operations.size() > 0 && operations.get(operations.size() - 1).isFinished()) {
            operations.add(new ClientOperation("read", key));
            log.info("[CLIENT " + id + "] Created new read operation");
        } else if (operations.size() == 0) {
            operations.add(new ClientOperation("read", key));
            log.info("[CLIENT " + id + "] Created new read operation");
        }

        //msg.printPath();
        getParent().tell(msg, getSelf());
        //CustomPrint.print(classString,"", String.valueOf(this.id), " Sent read request msg! to " + getParent());
        sendRequest();
        log.info("[CLIENT " + id + "] Sent read request msg! to " + getParent());
        startTimeout("read");
        log.info("[CLIENT " + id + "] Started read timeout");
    }

    public void onReadResponseMsg(Message.ReadResponseMsg msg){
        receivedResponse();
        //CustomPrint.print(classString,"", String.valueOf(this.id), " Received read response from " + getSender() + " with value " + msg.getValue() + " for key " + msg.getKey());
        log.info("[CLIENT " + id + "] Received read response from " + getSender() + " with value " + msg.getValue() + " for key " + msg.getKey());
        operations.get(operations.size() - 1).setValue(msg.getValue());
        operations.get(operations.size() - 1).setFinished(true);
        operations.get(operations.size() - 1).setSuccess(true);
        log.info("[CLIENT " + id + "] Operation " + operations.get(operations.size() - 1).getOperation() + " finished");
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
        if (!hasResponded()) {
            log.info("[CLIENT " + id + "] Received timeout msg from {}!", getSender().path().name());
            receivedResponse();
            log.info("[CLIENT " + id + "] Connecting to another L2 cache");
            Set<ActorRef> caches = getL2_caches();
            ActorRef[] tmpArray = caches.toArray(new ActorRef[caches.size()]);
            ActorRef cache = null;
            //possible improvement: treat the set as a circular array
            while(cache == this.parent || cache == null) {
                // generate a random number
                Random rnd = new Random();

                // this will generate a random number between 0 and
                // HashSet.size - 1
                int rndNumber = rnd.nextInt(caches.size());
                cache = tmpArray[rndNumber];
            }
            setParent(cache);
            log.info("[CLIENT " + id + "] New designated parent: " + getParent().path().name());
            getParent().tell(new RequestConnectionMsg(), getSelf());
            log.info("[CLIENT " + id + "] Sent request connection msg to " + getParent().path().name());
            startTimeout("connection");
            log.info("[CLIENT " + id + "] Started connection timeout");
        }
    }

    private void onTimeoutElapsedMsg (TimeoutElapsedMsg msg){
        log.info("[CLIENT " + id + "] Received timeout msg from {}!", getSender().path().name());
        receivedResponse();
    }

    public void onResponseConnectionMsg(ResponseConnectionMsg msg){
        log.info("[CLIENT " + id + "] Received response connection msg from " + getSender().path().name());
        receivedResponse();
        if(msg.getResponse().equals("ACCEPTED")){
            log.info("[CLIENT " + id + "] Connection established with " + getSender().path().name());
            retryOperation();
            log.info("[CLIENT " + id + "] Retrying operation");
        }

    }

    public void onStartReadRequestMsg(Message.StartReadRequestMsg msg) {
        CustomPrint.print(classString,"", String.valueOf(this.id), " Received start read request msg!");
        sendReadRequestMsg(msg.key);
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
