package it.unitn.ds1;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.*;

public class Message {

    // ----------GENERAL MESSAGES----------
    public static class InitMsg implements Serializable{
        public final ActorRef id;
        public final String type;

        public InitMsg(ActorRef id, String type) {
            this.id = id;
            this.type = type;
        }
    }

    public static class StartInitMsg implements Serializable{
        public StartInitMsg() {
        }
    }

    public static class TimeoutMsg implements Serializable{

        private final String type;
        private final long requestId;

        // variable to store the string of the name of the actor on the other side of the connection
        private final String connectionDestination;

        //for clients
        public TimeoutMsg(String type, String connectionDestination){
            this.type = type; //can be "read","write","connection"
            this.requestId = -1; //not used
            this.connectionDestination = connectionDestination;
        }

        //for caches
        public TimeoutMsg(String type, long requestId, String connectionDestination){
            this.type = type;
            this.requestId = requestId;
            this.connectionDestination = connectionDestination;
        }

        public String getType(){
            return this.type;
        }

        public long getRequestId(){
            return this.requestId;
        }

        public String getConnectionDestination() {
        	return this.connectionDestination;
        }

    }

    public static class InfoMsg implements Serializable{
        public InfoMsg() {}
    }

    // ----------CRASH RELATED MESSAGES----------
    public static class CrashMsg implements Serializable{
        public CrashMsg() {}
    }

    public static class RecoverMsg implements Serializable{
        public RecoverMsg() {}
    }

    public static class RequestDataRecoverMsg implements Serializable{
        public RequestDataRecoverMsg() {}
    }

    public static class ResponseDataRecoverMsg implements Serializable{
        private final Map<Integer, Integer> data;
        private final ActorRef parent;

        public ResponseDataRecoverMsg(Map<Integer, Integer> data, ActorRef parent) {
            this.parent = parent;
            this.data = data;
        }

        public Map<Integer, Integer> getData() {
            return this.data;
        }

        public ActorRef getParent() {
            return this.parent;
        }
    }

    public static class RequestUpdatedDataMsg implements Serializable{
        private final Set<Integer> keys;

        public RequestUpdatedDataMsg(Set<Integer> keys) {
            this.keys = keys;
        }

        public Set<Integer> getKeys() {
            return this.keys;
        }
    }

    public static class ResponseUpdatedDataMsg implements Serializable{
        private final Map<Integer, Integer> data;

        public ResponseUpdatedDataMsg(Map<Integer, Integer> data) {
            this.data = data;
        }

        public Map<Integer, Integer> getData() {
            return this.data;
        }
    }

    public static class UpdateDataMsg implements Serializable{
        private final Map<Integer, Integer> data;

        public UpdateDataMsg(Map<Integer, Integer> data){
            this.data = data;
        }

        public Map<Integer, Integer> getData() {
            return this.data;
        }
    }

    public static class RequestConnectionMsg implements Serializable{
        private String type;
        public RequestConnectionMsg(){}

        public RequestConnectionMsg(String type){
            this.type = type;
        }

        public String getType() {
            return this.type;
        }
    }

    public static class ResponseConnectionMsg implements Serializable{
        private String response;

        public ResponseConnectionMsg(){}

        public ResponseConnectionMsg(String response){
            this.response = response;
        }

        public String getResponse() {
            return this.response;
        }
    }

    public static class TimeoutElapsedMsg implements Serializable{
        private int key;
        private int value;
        private String type;

        public TimeoutElapsedMsg(){}

        public TimeoutElapsedMsg(int key, int value, String type){
            this.key = key;
            this.value = value;
            this.type = type;
        }

        public int getValue(){
            return this.value;
        }

        public int getKey(){
            return this.key;
        }

        public String getType() {
        	return this.type;
        }

        public void setKey(int key){
            this.key = key;
        }

        public void setValue(int value){
            this.value = value;
        }

        public void setType(String type) {
            this.type = type;
        }

    }


    // ----------DATABASE GENERAL MESSAGES----------
    public static class CurrentDataMsg implements Serializable {}

    public static class DropDatabaseMsg implements Serializable {}


    // ----------READ MESSAGES----------
    public static class DummyMsg implements Serializable {
        private final int payload;

        public DummyMsg(int payload) {
            this.payload = payload;
        }

        public int getPayload() {
            return payload;
        }

    }

    //NOTE: test if really needed
    public static class StartReadMsg implements Serializable {
        private final int key;

        public StartReadMsg(int key){
            this.key = key;
        }

        public int getKey() {
            return this.key;
        }
    }

    public static class ReadRequestMsg implements Serializable {
        private final int key;
        private final Stack<ActorRef> path;
        private final long requestId;

        public ReadRequestMsg(int key, Stack<ActorRef> path, long requestId) {
            this.key = key;
            //path should be unmodifiable, to follow the general akka rule
            this.path = new Stack<>();
            this.path.addAll(path);
            this.requestId = requestId;
        }

        public long getRequestId() {
            return requestId;
        }

        public int getKey() {
            return key;
        }

        public Stack<ActorRef> getPath() {
            return path;
        }

        //get last element of the path
        public ActorRef getLast() {
            return path.peek();
        }

        //get path size
        public int getPathSize() {
            return path.size();
        }

        // Print the path
        public String printPath() {
            StringBuilder sb = new StringBuilder();
            sb.append("Current path of message: [ ");
            for (ActorRef actor : path) {
                sb.append(actor.path().name()).append(" ");
            }
            sb.append(" ]");
            return sb.toString();
        }

    }

    public static class ReadResponseMsg implements Serializable {
        private final int key;
        private final int value;
        private final Stack<ActorRef> path;
        private final long requestId;

        public ReadResponseMsg(int key, int value, List<ActorRef> path, long requestId) {
            this.key = key;
            this.value = value;
            //path should be unmodifiable, to follow the general akka rule
            this.path = new Stack<>();
            this.path.addAll(path);
            this.requestId = requestId;
        }

        public int getKey() {
            return key;
        }

        public int getValue() {
            return value;
        }

        public long getRequestId() {
            return requestId;
        }

        public Stack<ActorRef> getPath() {
            return path;
        }

        //get last element of the path
        public ActorRef getLast() {
            if (path == null || path.isEmpty()) {
                return null;
            }
            return path.peek();
        }

        //get path size
        public int getPathSize() {
            return path.size();
        }

        // Print the path
        public String printPath() {
            StringBuilder sb = new StringBuilder();
            sb.append("Current path of message: [ ");
            for (ActorRef actor : path) {
                sb.append(actor.path().name()).append(" ");
            }
            sb.append(" ]");
            return sb.toString();
        }

    }



    // ----------WRITE MESSAGES----------
    public static class WriteResponseMsg implements Serializable {
        private final int key;
        private final int value;
        public Stack<ActorRef> path;
        private final long requestId;

        public WriteResponseMsg(int key, int value, Stack<ActorRef> path, long requestId) {
            this.key = key;
            this.value = value;
            this.path = path;
            this.requestId = requestId;
        }

        public int getKey() {
            return key;
        }

        public int getValue() {
            return value;
        }

        public long getRequestId() {
            return requestId;
        }

        public Stack<ActorRef> getPath() {
            return path;
        }

        public int getPathSize() {
            return path.size();
        }

        // Print the path
        public String printPath() {
            StringBuilder sb = new StringBuilder();
            sb.append("Current path of message: [ ");
            for (ActorRef actor : path) {
                sb.append(actor.path().name()).append(" ");
            }
            sb.append(" ]");
            return sb.toString();
        }
    }

    public static class CritReadResponseMsg implements Serializable {
        private final int key;
        private final int value;
        private final Stack<ActorRef> path;
        private final long requestId;

        public CritReadResponseMsg(int key, int value, List<ActorRef> path, long requestId) {
            this.key = key;
            this.value = value;
            //path should be unmodifiable, to follow the general akka rule
            this.path = new Stack<>();
            this.path.addAll(path);
            this.requestId = requestId;
        }

        public int getKey() {
            return key;
        }

        public int getValue() {
            return value;
        }

        public long getRequestId() {
            return requestId;
        }

        public Stack<ActorRef> getPath() {
            return path;
        }

        //get last element of the path
        public ActorRef getLast() {
            if (path == null || path.isEmpty()) {
                return null;
            }
            return path.peek();
        }

        //get path size
        public int getPathSize() {
            return path.size();
        }

        // Print the path
        public String printPath() {
            StringBuilder sb = new StringBuilder();
            sb.append("Current path of message: [ ");
            for (ActorRef actor : path) {
                sb.append(actor.path().name()).append(" ");
            }
            sb.append(" ]");
            return sb.toString();
        }

    }

    public static class StartReadRequestMsg implements Serializable {
        public final int key;

        public StartReadRequestMsg(int key) {
            this.key = key;
        }
    }

    public static class StartWriteMsg implements Serializable{ //TODO: change into private with getters
        public final int key;
        public final int value;

        public StartWriteMsg(int key, int value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class WriteRequestMsg implements Serializable{
        public final int key;
        public final int value;
        public Stack<ActorRef> path;
        private final long requestId;

        public WriteRequestMsg(int key, int value, Stack<ActorRef> path, long requestId) {
            this.key = key;
            this.value = value;
            //path should be unmodifiable, to follow the general akka rule
            this.path = new Stack<>();
            this.path.addAll(path);
            this.requestId = requestId;
        }

        public long getRequestId() {
            return requestId;
        }

        public int getKey() {
            return key;
        }

        public int getValue() {
            return value;
        }

        public Stack<ActorRef> getPath() {
            return path;
        }

        //get last element of the path
        public ActorRef getLast() {
            return path.peek();
        }

        //get path size
        public int getPathSize() {
            return path.size();
        }

        // Print the path
        public String printPath() {
            StringBuilder sb = new StringBuilder();
            sb.append("Current path of message: [ ");
            for (ActorRef actor : path) {
                sb.append(actor.path().name()).append(" ");
            }
            sb.append(" ]");
            return sb.toString();
        }
    }


    public static class FillMsg implements Serializable{ //TODO: private variables
        public final int key;
        public final int value;

        public FillMsg(int key, int value) {
            this.key = key;
            this.value = value;
        }

        public int getKey() {
            return key;
        }

        public int getValue() {
            return value;
        }
    }

    public static class InfoItemsMsg implements Serializable {}












}
