package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Props;

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

    // ----------L1 CACHES LOGIC----------

    public Set<ActorRef> getL1_caches() {
        return this.L1_caches;
    }

    public void setL1_caches(Set<ActorRef> l1_caches) {
        this.L1_caches = l1_caches;
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

        CustomPrint.print(classString,"Database " + id + " started");
        CustomPrint.print(classString, "Initial data in database " + id + ":");
        for (Map.Entry<Integer, Integer> entry : data.entrySet()) {
            CustomPrint.print(classString, "Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }

    public void populateDatabase() {
        for (int i = 0; i < 10; i++) {
            data.put(i, rnd.nextInt(200));
        }
        CustomPrint.debugPrint(classString, "Database " + id + " populated");
    }




    public void onReadRequestMsg(ReadRequestMsg msg) {
        CustomPrint.debugPrint(classString, "Database " + id + " received a read request for key " + msg.key + " from client " + msg.clientID);
        int value = data.get(msg.key);
        CustomPrint.debugPrint(classString, "Database " + id + " read value " + value + " for key " + msg.key);

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

    public void onWriteRequestMsg(WriteRequestMsg msg) {
        CustomPrint.debugPrint(classString, "Database " + id + " received a write request for key " + msg.key + " with value " + msg.value);

        //data.put(msg.key, msg.value);
        //CustomPrint.debugPrint(classString, "Database " + id + " wrote key " + msg.key + " with value " + msg.value);

        // notify all L1 caches
        //for (ActorRef cache : L1_caches) {}

    }

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


    // Here we define the mapping between the received message types and the database methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(CurrentDataMsg.class, this::onCurrentDataMsg)
            .match(DropDatabaseMsg.class, this::onDropDatabaseMsg)
            .match(ReadRequestMsg.class, this::onReadRequestMsg)
            .match(WriteRequestMsg.class, this::onWriteRequestMsg)
            .matchAny(o -> System.out.println("Received unknown message from " + getSender()))
            .build();
    }

}
