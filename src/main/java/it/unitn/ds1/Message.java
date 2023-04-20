package it.unitn.ds1;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.*;

public class Message {

    // ----------GENERAL MESSAGES----------
    public static class InitMsg implements Serializable{
        private final ActorRef id;
        private final String type;

        public InitMsg(ActorRef id, String type) {
            this.id = id;
            this.type = type;
        }

        public ActorRef getId() {
            return this.id;
        }

        public String getType() {
            return this.type;
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

    public static class TimeoutElapsedMsg implements Serializable{
        private int key;
        private int value;

        public TimeoutElapsedMsg(){}

        public TimeoutElapsedMsg(int key, int value){
            this.key = key;
            this.value = value;
        }

        public void setKey(int key){
            this.key = key;
        }

        public void setValue(int value){
            this.value = value;
        }

        public int getValue(){
            return this.value;
        }

        public int getKey(){
            return this.key;
        }
    }

    // ----------DATABASE GENERAL MESSAGES----------
    public static class CurrentDataMsg implements Serializable {}

    public static class DropDatabaseMsg implements Serializable {}

    // ----------READ MESSAGES----------

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
        private final int clientID; //maybe not needed

        public ReadRequestMsg(int key, int clientID) {
            this.key = key;
            this.clientID = clientID;
        }

        public int getKey() {
            return this.key;
        }

        public int getClientID() {
            return this.clientID;
        }
    }

    public static class ReadConfirmationMsg implements Serializable {
        private final int key;
        private final int value;
        private final int clientID;

        public ReadConfirmationMsg(int key, int value, int clientID) {
            this.key = key;
            this.value = value;
            this.clientID = clientID;
        }

        public int getKey() {
            return this.key;
        }

        public int getValue() {
            return this.value;
        }

        public int getClientID() {
            return this.clientID;
        }
    }
    
    public static class StartCriticalReadMsg implements Serializable {
        private final int key;
        
        public StartCriticalReadMsg(int key){
            this.key = key;
        }

        public int getKey() {
            return this.key;
        }
    }

    public static class CriticalReadRequestMsg implements Serializable {
        private final int key;
        private final Stack<ActorRef> path;

        public CriticalReadRequestMsg(int key, Stack<ActorRef> path){
            this.key = key;
            this.path = path;
        }

        public Stack<ActorRef> getPath(){
            Stack<ActorRef> stack = new Stack<>();
            stack.addAll(this.path);
            return stack;
        }

        public int getKey() {
            return this.key;
        }
    }

    public static class CriticalReadResponseMsg implements Serializable {
        private final int key;
        private final int value;
        private final Stack<ActorRef> path;

        public CriticalReadResponseMsg(int key, int value, Stack<ActorRef> path){
            this.key = key;
            this.value = value;
            this.path = path;
        }

        public Stack<ActorRef> getPath(){
            Stack<ActorRef> stack = new Stack<>();
            stack.addAll(this.path);
            return stack;
        }

        public int getKey() {
            return this.key;
        }

        public int getValue() {
            return this.value;
        }
    }

    // ----------WRITE MESSAGES----------
    public static class WriteResponseMsg implements Serializable {
        private final int key;
        private final int value;
        private final Stack<ActorRef> path;


        public WriteResponseMsg(int key, int value, Stack<ActorRef> path) {
            this.key = key;
            this.value = value;
            this.path = path;
        }

        public Stack<ActorRef> getPath(){
            Stack<ActorRef> stack = new Stack<>();
            stack.addAll(path);
            return stack;
        }

        public int getKey() {
            return this.key;
        }

        public int getValue() {
            return this.value;
        }
    }

    public static class StartWriteMsg implements Serializable{
        private final int key;
        private final int value;

        public StartWriteMsg(int key, int value) {
            this.key = key;
            this.value = value;
        }

        public int getValue() {
            return this.value;
        }

        public int getKey() {
            return this.key;
        }
    }

    public static class WriteRequestMsg implements Serializable{
        private final int key;
        private final int value;
        private final Stack<ActorRef> path;


        public WriteRequestMsg(int key, int value, Stack<ActorRef> path) {
            this.key = key;
            this.value = value;
            this.path = path;
        }

        public Stack<ActorRef> getPath(){
            Stack<ActorRef> stack = new Stack<>();
            stack.addAll(this.path);
            return stack;
        }

        public int getKey() {
            return this.key;
        }

        public int getValue() {
            return this.value;
        }
    }

    public static class CriticalWriteResponseMsg implements Serializable {
        private final int key;
        private final int value;
        private final Stack<ActorRef> path;

        public CriticalWriteResponseMsg(int key, int value, Stack<ActorRef> path) {
            this.key = key;
            this.value = value;
            this.path = path;
        }

        public Stack<ActorRef> getPath(){
            Stack<ActorRef> stack = new Stack<>();
            stack.addAll(path);
            return stack;
        }

        public int getKey() {
            return this.key;
        }

        public int getValue() {
            return this.value;
        }
    }

    public static class StartCriticalWriteMsg implements Serializable{
        private final int key;
        private final int value;

        public StartCriticalWriteMsg(int key, int value) {
            this.key = key;
            this.value = value;
        }

        public int getValue() {
            return this.value;
        }

        public int getKey() {
            return this.key;
        }
    }

    public static class CriticalWriteRequestMsg implements Serializable{
        private final int key;
        private final int value;
        private final Stack<ActorRef> path;

        public CriticalWriteRequestMsg(int key, int value, Stack<ActorRef> path) {
            this.key = key;
            this.value = value;
            this.path = path;
        }

        public Stack<ActorRef> getPath(){
            Stack<ActorRef> stack = new Stack<>();
            stack.addAll(this.path);
            return stack;
        }

        public int getKey() {
            return this.key;
        }

        public int getValue() {
            return this.value;
        }
    }

    public static class FillMsg implements Serializable{
        private final int key;

        private final int value;

        public FillMsg(int key, int value) {
            this.key = key;
            this.value = value;
        }

        public int getKey() {
            return this.key;
        }

        public int getValue() {
            return this.value;
        }
    }

    public static class RefillMsg implements Serializable{
        private final int key;
        private final int value;

        public RefillMsg(int key, int value){
            this.key = key;
            this.value = value;
        }

        public int getKey() {
            return this.key;
        }

        public int getValue() {
            return this.value;
        }
    }

    public static class RefillResponseMsg implements Serializable{
        private final int key;
        private final int value;

        public RefillResponseMsg(int key, int value){
            this.key = key;
            this.value = value;
        }

        public int getKey() {
            return this.key;
        }

        public int getValue() {
            return this.value;
        }
    }
}
