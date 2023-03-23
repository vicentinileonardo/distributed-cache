package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import it.unitn.ds1.Message.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Thread.sleep;

import org.yaml.snakeyaml.Yaml;

public class DistributedCacheSystem {

    public static void main(String[] args) throws IOException {

        String configFilePath = System.getProperty("user.dir") + "/config.yaml";

        System.out.println("Loading config from: " + configFilePath);

        InputStream inputStream = new FileInputStream(new File(configFilePath));
        Yaml yaml = new Yaml();
        Map<String, Object> configuration = yaml.load(inputStream);

        int N_CLIENTS = (int) configuration.get("N_CLIENTS");
        int N_L2_CACHES = (int) configuration.get("N_L2_CACHES");
        int N_L1_CACHES = (int) configuration.get("N_L1_CACHES");

        System.out.println("N_CLIENTS: " + N_CLIENTS);
        System.out.println("N_L2_CACHES: " + N_L2_CACHES);
        System.out.println("N_L1_CACHES: " + N_L1_CACHES);


        final ActorSystem system = ActorSystem.create("distributed_cache_system");

        // create the database
        final ActorRef database = system.actorOf(Database.props(0), "database");

        //read the YAML config file and create the caches
        //TODO

        //create the clients
        List<ActorRef> group = new ArrayList<>();
        for (int i = 0; i < N_CLIENTS; i++) {
            group.add(system.actorOf(Client.props(i), "client" + i));
        }


        //examples
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
