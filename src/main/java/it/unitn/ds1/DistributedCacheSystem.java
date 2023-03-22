package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import it.unitn.ds1.Database.CurrentDataMsg;
import it.unitn.ds1.Database.ReadRequestMsg;
import it.unitn.ds1.Database.WriteRequestMsg;


import java.io.IOException;

import static java.lang.Thread.sleep;

public class DistributedCacheSystem {

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("distributed_cache_system");

        // create the database
        final ActorRef database = system.actorOf(Database.props(0), "database");

        //read the YAML config file and create the caches
        //TODO

        //create the clients
        //TODO

        //read requests msg to the database
        ReadRequestMsg readRequestMsg = new ReadRequestMsg(4, 0);
        database.tell(readRequestMsg, ActorRef.noSender());

        //write requests msg to the database
        WriteRequestMsg writeRequestMsg = new WriteRequestMsg(5, 5, 3);
        database.tell(writeRequestMsg, ActorRef.noSender());

        //current database state
        CurrentDataMsg currentDataMsg = new CurrentDataMsg();
        database.tell(currentDataMsg, ActorRef.noSender());




        try {
            sleep(2000);
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        }
        catch (IOException ioe) {} catch (InterruptedException e) {
            e.printStackTrace();
        }
        system.terminate();

    }
}
