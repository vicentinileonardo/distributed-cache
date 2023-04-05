package it.unitn.ds1;

import akka.actor.Actor;
import akka.actor.ActorRef;

import java.io.Serializable;
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

    public static class CacheReadRequestMsg implements Serializable {

        public final int key;
        public final ActorRef sender;
        public final ActorRef L1cache; //this or cacheID?
        public final ActorRef L2cache;
        public final ActorRef client;

        //created by l2cache
        public CacheReadRequestMsg(ClientReadRequestMsg clientMsg, ActorRef self) {
            this.key = clientMsg.key;
            this.sender = self;
            this.L1cache = null;
            this.L2cache = self;
            this.client = clientMsg.client;
        }

        //created by l1cache
        public CacheReadRequestMsg(CacheReadRequestMsg cacheMsg, ActorRef self) {
            this.key = cacheMsg.key;
            this.sender = self;
            this.L1cache = self;
            this.L2cache = cacheMsg.L2cache;
            this.client = cacheMsg.client;

        }
    }

    public static class ClientReadResponseMsg implements Serializable {
        public final int key;
        public final int value;

        public ClientReadResponseMsg(int key, int value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class CacheReadResponseMsg implements Serializable {
        public final int key;
        public final int value;
        public final ActorRef L1cache;
        public final ActorRef L2cache;
        public final ActorRef client;

        public CacheReadResponseMsg(int key, int value, ActorRef L1cache, ActorRef L2cache, ActorRef client) {
            this.key = key;
            this.value = value;
            this.L1cache = L1cache;
            this.L2cache = L2cache;
            this.client = client;
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
