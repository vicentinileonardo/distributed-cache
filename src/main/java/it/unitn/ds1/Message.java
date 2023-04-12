package it.unitn.ds1;

import akka.actor.Actor;
import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.HashMap;
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
