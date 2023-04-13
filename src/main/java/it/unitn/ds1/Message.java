package it.unitn.ds1;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

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
        public TimeoutMsg() {

        }
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
        public Map<Integer, Integer> data;
        public ActorRef parent;

        public ResponseDataRecoverMsg(Map<Integer, Integer> data, ActorRef parent) {
            this.parent = parent;
            this.data = data;
        }
    }

    public static class RequestUpdatedDataMsg implements Serializable{
        Set<Integer> keys;

        public RequestUpdatedDataMsg(Set<Integer> keys) {
            this.keys = keys;
        }
    }

    public static class ResponseUpdatedDataMsg implements Serializable{
        Map<Integer, Integer> data;

        public ResponseUpdatedDataMsg(Map<Integer, Integer> data) {
            this.data = data;
        }
    }

    public static class UpdateDataMsg implements Serializable{
        Map<Integer, Integer> data;

        public UpdateDataMsg(Map<Integer, Integer> data){
            this.data = data;
        }
    }

    public static class RequestConnectionMsg implements Serializable{
        String type;
        public RequestConnectionMsg(){}

        public RequestConnectionMsg(String type){
            this.type = type;
        }
    }

    public static class TimeoutElapsedMsg implements Serializable{
        public TimeoutElapsedMsg(){}
    }

    // ----------DATABASE GENERAL MESSAGES----------
    public static class CurrentDataMsg implements Serializable {}

    public static class DropDatabaseMsg implements Serializable {}

    // ----------READ MESSAGES----------
    public static class ReadRequestMsg implements Serializable {
        public final int key;
        public final int clientID; //maybe not needed

        public ReadRequestMsg(int key, int clientID) {
            this.key = key;
            this.clientID = clientID;
        }
    }

    public static class ReadConfirmationMsg implements Serializable {
        public final int key;
        public final int value;
        public final int clientID;

        public ReadConfirmationMsg(int key, int value, int clientID) {
            this.key = key;
            this.value = value;
            this.clientID = clientID;
        }
    }
    
    public static class StartCriticalReadMsg implements Serializable {
        public final int key;
        
        public StartCriticalReadMsg(int key){
            this.key = key;
        }
    }

    public static class CriticalReadRequestMsg implements Serializable {
        public final int key;
        public Stack<ActorRef> path;

        public CriticalReadRequestMsg(int key, Stack<ActorRef> path){
            this.key = key;
            this.path = path;
        }
    }

    public static class CriticalReadResponseMsg implements Serializable {
        public final int key;
        public final int value;
        public Stack<ActorRef> path;

        public CriticalReadResponseMsg(int key, int value, Stack<ActorRef> path){
            this.key = key;
            this.value = value;
            this.path = path;
        }
    }

    // ----------WRITE MESSAGES----------
    public static class WriteResponseMsg implements Serializable {
        public final int key;
        public final int value;
        public Stack<ActorRef> path;

        public WriteResponseMsg(int key, int value, Stack<ActorRef> path) {
            this.key = key;
            this.value = value;
            this.path = path;
        }
    }

    public static class StartWriteRequestMsg implements Serializable{
        public final int key;
        public final int value;

        public StartWriteRequestMsg(int key, int value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class WriteRequestMsg implements Serializable{
        public final int key;
        public final int value;
        public Stack<ActorRef> path;

        public WriteRequestMsg(int key, int value, Stack<ActorRef> path) {
            this.key = key;
            this.value = value;
            this.path = path;
        }
    }

    public static class FillMsg implements Serializable{
        public final int key;
        public final int value;
        public FillMsg(int key, int value) {
            this.key = key;
            this.value = value;
        }
    }
}
