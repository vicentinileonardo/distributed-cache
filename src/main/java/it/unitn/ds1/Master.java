package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class Master extends AbstractActor {

	ActorRef database;

	private ActorRef getDatabase() {
		return this.database;
	}

	HashSet<ActorRef> l1Caches;

	private HashSet<ActorRef> getL1Caches() {
		return this.l1Caches;
	}

	HashSet<ActorRef> l2Caches;

	private HashSet<ActorRef> getL2Caches() {
		return this.l2Caches;
	}

	HashMap<Integer, Integer> data;

	private void addData(Map<Integer, Integer> newData) {
		this.data.putAll(newData);
	}

	private HashMap<Integer, Integer> getData(){
		return this.data;
	}

	HashSet<ActorRef> requestsSent;

	private void addRequest(ActorRef actor){
		this.requestsSent.add(actor);
	}

	private void removeRequest(ActorRef actor){
		this.requestsSent.remove(actor);
	}

	private boolean isRequestsEmpty(){
		return this.requestsSent.isEmpty();
	}

	boolean isConsistent = true;

	private boolean getConsistentStatus() {
		return this.isConsistent;
	}

	private void setInconsistentStatus(){
		this.isConsistent = false;
	}

	public Master(ActorRef db, HashSet<ActorRef> l1, HashSet<ActorRef> l2) {
		this.database = db;
		this.l1Caches = new HashSet<ActorRef>();
		this.l1Caches.addAll(l1);
		this.l2Caches = new HashSet<ActorRef>();
		this.l2Caches.addAll(l2);
	}

	static public Props props(ActorRef db, HashSet<ActorRef> l1, HashSet<ActorRef> l2) {
		return Props.create(Master.class, new Master(db, l1, l2));
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(Message.StartHealthCheck.class, this::onStartHealthCheck)
				.match(Message.HealthCheckResponseMsg.class, this::onHealthCheckResponse)
				.matchAny(o -> System.out.println("Master received unknown message from " + getSender()))
				.build();
	}

	private void onStartHealthCheck(Message.StartHealthCheck msg){
		Message.HealthCheckRequestMsg new_msg = new Message.HealthCheckRequestMsg();
		getDatabase().tell(new_msg, getSelf());
	}

	private void onHealthCheckResponse(Message.HealthCheckResponseMsg msg){
		if (getSender() == database){
			addData(msg.getData());

			Message.HealthCheckRequestMsg new_msg = new Message.HealthCheckRequestMsg();
			for (ActorRef cache : getL1Caches()) {
				cache.tell(new_msg, self());
				addRequest(cache);
			}

			for (ActorRef cache : getL2Caches()) {
				cache.tell(new_msg, self());
				addRequest(cache);
			}

		} else {
			boolean currentConsistency = true;
			for (Map.Entry<Integer, Integer> entry : msg.getData().entrySet()) {
				if (entry.getValue() != getData().get(entry.getKey())) {
					setInconsistentStatus();
					if (currentConsistency){
						System.out.println("Actor "+getSender().toString()+" is inconsistent!");
						currentConsistency = false;
					}
					System.out.println("Inconsistent value at key: " + entry.getKey() +
							" -> Actor value: " + entry.getValue() + "|| DB value: " + getData().get(entry.getKey()));
				}
			}

			removeRequest(getSender());
		}

		if (isRequestsEmpty()) {
			System.out.println("Is the system consistent? "+ getConsistentStatus());
		}
	}

}
