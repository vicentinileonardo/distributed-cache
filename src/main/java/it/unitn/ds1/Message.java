package it.unitn.ds1;

import akka.actor.Actor;
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

    // ----------DATABASE GENERAL MESSAGES----------
    public static class CurrentDataMsg implements Serializable {}

    public static class DropDatabaseMsg implements Serializable {}

    // ----------READ MESSAGES----------

    public static class ClientReadRequestMsg implements Serializable {
        public final int key;
        public final ActorRef client; //this or clientID?

        public ClientReadRequestMsg(int key, ActorRef client) {
            this.key = key;
            this.client = client;
        }
    }

    public static class DummyMsg implements Serializable {
        private final int payload;

        public DummyMsg(int payload) {
            this.payload = payload;
        }

        public int getPayload() {
            return payload;
        }

    }



    public static class ReadRequestMsg implements Serializable {
        private final int key;
        private final Stack<ActorRef> path;

        public ReadRequestMsg(int key, Stack<ActorRef> path) {
            this.key = key;

            //path should be unmodifiable, to follow the general akka rule
            this.path = new Stack<>();
            this.path.addAll(path);

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

        public ReadResponseMsg(int key, int value, List<ActorRef> path) {
            this.key = key;
            this.value = value;
            //path should be unmodifiable, to follow the general akka rule
            this.path = new Stack<>();
            this.path.addAll(path);
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
    public static class WriteConfirmationMsg implements Serializable {
        public final int key;
        public final int value;
        public Stack<ActorRef> path;

        public WriteConfirmationMsg(int key, int value, Stack<ActorRef> path) {
            this.key = key;
            this.value = value;
            this.path = path;
        }
    }

    public static class StartReadRequestMsg implements Serializable {
        public final int key;

        public StartReadRequestMsg(int key) {
            this.key = key;
        }
    }

    public static class StartWriteMsg implements Serializable{
        public final int key;
        public final int value;

        public StartWriteMsg(int key, int value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class WriteMsg implements Serializable{
        public final int key;
        public final int value;
        public Stack<ActorRef> path;

        public WriteMsg(int key, int value, Stack<ActorRef> path) {
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
