package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class Master extends AbstractActor {

	ActorRef database;
	HashSet<ActorRef> l1Caches;
	HashSet<ActorRef> l2Caches;

	HashMap<Integer, Integer> data;

	HashSet<ActorRef> requestsSent;

	boolean isConsistent = true;

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
		this.database.tell(new_msg, getSelf());
	}

	private void onHealthCheckResponse(Message.HealthCheckResponseMsg msg){
		if (getSender() == database){
			this.data.putAll(msg.getData());

			Message.HealthCheckRequestMsg new_msg = new Message.HealthCheckRequestMsg();
			for (ActorRef cache : l1Caches) {
				cache.tell(new_msg, self());
				this.requestsSent.add(cache);
			}

			for (ActorRef cache : l2Caches) {
				cache.tell(new_msg, self());
				this.requestsSent.add(cache);
			}

		} else {
			for (Map.Entry<Integer, Integer> entry : msg.getData().entrySet()) {
				if (entry.getValue() != this.data.get(entry.getKey())) {
					this.isConsistent = false;
					System.out.println("Actor "+getSender().toString()+" is inconsistent!");
					break;
				}
			}

			this.requestsSent.remove(getSender());
		}

		if (this.requestsSent.isEmpty()) {
			System.out.println("Is the system consistent? "+ this.isConsistent);
		}
	}

}
