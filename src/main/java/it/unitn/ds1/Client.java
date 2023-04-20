package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.Await;
import akka.util.Timeout;
import akka.pattern.*;
import scala.concurrent.duration.*;

import java.util.*;

public class Client extends AbstractActor {

    private int id;

    //DEBUG ONLY: assumption that clients are always up
    private boolean crashed = false;

    private HashSet<ActorRef> L2_caches = new HashSet<>();

    private ActorRef parent;

    private HashMap<String, Integer> timeouts = new HashMap<>();

    private Random rnd = new Random();
    private String classString = String.valueOf(getClass());

    public Client(int id, ActorRef parent, List<TimeoutConfiguration> timeouts, HashSet<ActorRef> l2Caches) {
        this.id = id;
        setParent(parent);
        setTimeouts(timeouts);
        setL2_caches(l2Caches);
    }

    static public Props props(int id, ActorRef parent, List<TimeoutConfiguration> timeouts, HashSet<ActorRef> l2Caches) {
        return Props.create(Client.class, () -> new Client(id, parent, timeouts, l2Caches));
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
        Stack<ActorRef> path = new Stack<>();
        path.push(getSelf());
        Message.ReadRequestMsg msg = new Message.ReadRequestMsg(key, path);
        msg.printPath();
        getParent().tell(msg, getSelf());
        CustomPrint.print(classString,"", String.valueOf(this.id), " Sent read request msg! to " + getParent());
    }

    public void onReadResponseMsg(Message.ReadResponseMsg msg){
        CustomPrint.print(classString,"", String.valueOf(this.id), " Received read response from " + getSender() + " with value " + msg.getValue() + " for key " + msg.getKey());
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
                .match(Message.StartInitMsg.class, this::onStartInitMsg)
                .match(Message.StartReadRequestMsg.class, this::onStartReadRequestMsg)
                .match(Message.StartWriteMsg.class, this::onStartWriteMsg)
                .match(Message.ReadResponseMsg.class, this::onReadResponseMsg)
                .match(Message.WriteConfirmationMsg.class, this::onWriteConfirmationMsg)
                .matchAny(o -> System.out.println("Client " + id +" received unknown message from " + getSender()))
                .build();
    }

    private void onStartInitMsg(Message.StartInitMsg msg) {
        CustomPrint.print(classString,"", String.valueOf(this.id), " Received initialization msg!");
        sendInitMsg();
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
