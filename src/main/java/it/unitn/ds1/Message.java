package it.unitn.ds1;

import akka.actor.Actor;
import akka.actor.ActorRef;

import java.io.Serializable;

public class Message {

    public static class CurrentDataMsg implements Serializable {}

    public static class DropDatabaseMsg implements Serializable {}

    public static class WriteRequestMsg implements Serializable {
        public final int key;
        public final int value;
        public final int clientID; //maybe not needed

        public WriteRequestMsg(int key, int value, int clientID) {
            this.key = key;
            this.value = value;
            this.clientID = clientID;
        }
    }

    public static class ReadRequestMsg implements Serializable {
        public final int key;
        public final int clientID; //maybe not needed

        public ReadRequestMsg(int key, int clientID) {
            this.key = key;
            this.clientID = clientID;
        }
    }

    public static class WriteConfirmationMsg implements Serializable {
        public final int key;
        public final int value;
        public final int clientID;

        public WriteConfirmationMsg(int key, int value, int clientID) {
            this.key = key;
            this.value = value;
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
}
