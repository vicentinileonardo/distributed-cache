package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import it.unitn.ds1.Message.*;

import java.io.*;
import java.util.*;

import static akka.http.javadsl.server.Directives.concat;
import static java.lang.Thread.sleep;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import akka.http.javadsl.Http;
import akka.http.javadsl.server.Route;

public class DistributedCacheSystem {

    private final String config_file;
    private Configuration configuration;

    private ActorSystem system;
    private ActorRef databaseActor;
    private HashSet<ActorRef> l1CacheActors;
    private HashSet<ActorRef> l2CacheActors;
    private HashSet<ActorRef> clientActors;

    private ActorRef master;

    public DistributedCacheSystem(String config_file) {
        this.config_file = config_file;
    }

    //getter for l2 cache actors
    public HashSet<ActorRef> getL2Caches() {
        return l2CacheActors;
    }

    //getter for l1 cache actors
    public HashSet<ActorRef> getL1Caches() {
        return l1CacheActors;
    }

    //getter for client actors
    public HashSet<ActorRef> getClients() {
        return clientActors;
    }

    public void parse() throws IOException {
        String configFilePath = System.getProperty("user.dir") + "/"+ this.config_file;

        System.out.println("Loading config from: " + configFilePath);


        Yaml yaml = new Yaml(new Constructor(Configuration.class));
        System.out.println("New yaml object!");
        InputStream inputStream = new FileInputStream(configFilePath);

        System.out.println("Read yaml file!");
        this.configuration = yaml.load(inputStream);
        System.out.println("Parsed config file!");

    }

    private int randomRangeRandom(int max) {
        int start = 1;
        Random random = new Random();
        return random.nextInt((max - start) + 1) + start;
    }

    public void buildCustomSystem(){
        this.system = ActorSystem.create("distributed_cache_system");
        System.out.println("Custom system creation!");
        // Build database
        this.databaseActor = system.actorOf(Database.props(0, configuration.getDatabase().getTimeouts()));

        // Build L1 caches up to maxNum

        this.l1CacheActors = new HashSet<>();
        int totalL1Num = configuration.getL1Caches().getCustomNum();
        for (int i = 0; i < totalL1Num; i++) {
            this.l1CacheActors.add(system.actorOf(Cache.props(i,
                    "L1",
                    databaseActor,
                    configuration.getL1Caches().getTimeouts())));
        }

        // Build L2 caches up to maxNum for each L1 cache
        this.l2CacheActors = new HashSet<>();
        int totalL2Caches = 0;
        int l2Num = 0;
        int totalL2Num = configuration.getL2Caches().getCustomNum();
        int l2CachesPerL1Cache = totalL2Num / totalL1Num;
        int l2CacheToSpare = totalL2Num % totalL1Num;

        for (ActorRef l1Cache : l1CacheActors) {
            if (l2CachesPerL1Cache == 0 && l2CacheToSpare == 0) {
                continue;
            } else if (l2CacheToSpare == 0) {
                l2Num = l2CachesPerL1Cache;
            } else {
                l2Num = l2CachesPerL1Cache + 1;
                l2CacheToSpare--;
            }
            for (int i = 0; i < l2Num; i++) {
                this.l2CacheActors.add(system.actorOf(Cache.props(i+totalL2Caches,
                        "L2",
                        l1Cache,
                        databaseActor,
                        configuration.getL2Caches().getTimeouts())));
            }
            totalL2Caches += l2Num;
        }

        // Build clients up to maxNum for each L2 cache
        this.clientActors = new HashSet<>();
        int totalClients = 0;
        int clientNum = 0;

        int totalClientsNum = configuration.getClients().getCustomNum();
        int clientsPerL2Cache = totalClientsNum / totalL2Num;
        int clientsToSpare = totalClientsNum % totalL2Num;

        for (ActorRef l2Cache : l2CacheActors) {
            if (clientsPerL2Cache == 0 && clientsToSpare == 0) {
                continue;
            } else if (clientsToSpare == 0) {
                clientNum = clientsPerL2Cache;
            } else {
                clientNum = clientsPerL2Cache + 1;
                l2CacheToSpare--;
            }
            for (int i = 0; i < clientNum; i++) {
                this.clientActors.add(system.actorOf(Client.props(i+totalClients,
                        l2Cache,
                        configuration.getClients().getTimeouts(),
                        l2CacheActors)));
            }
            totalClients += clientNum;
        }
        System.out.println("Client " + (totalClients == totalClientsNum) );

        this.master = system.actorOf(Master.props(this.l1CacheActors, this.l2CacheActors, this.clientActors), "master");
    }

    public void buildAutoSystem(){

        System.out.println("Automatic system creation!");
        boolean isUnbalanced = configuration.getSystemProperty().getUnbalanced();
        this.system = ActorSystem.create("distributed_cache_system");

        // Build database
        this.databaseActor = system.actorOf(Database.props(0, configuration.getDatabase().getTimeouts()));

        // Build L1 caches up to maxNum
        int l1_num;
        if (isUnbalanced) {
            l1_num = randomRangeRandom(configuration.getL1Caches().getMaxNum());
        } else {
            l1_num = configuration.getL1Caches().getMaxNum();
        }

        this.l1CacheActors = new HashSet<>();
        for (int i = 0; i < l1_num; i++) {
            this.l1CacheActors.add(system.actorOf(Cache.props(i,
                    "L1",
                    databaseActor,
                    configuration.getL1Caches().getTimeouts())));
        }

        // Build L2 caches up to maxNum for each L1 cache
        this.l2CacheActors = new HashSet<>();
        int total_l2_caches = 0;
        int l2_num = 0;
        boolean randomL2Num = false;
        if (isUnbalanced) {
            randomL2Num = true;
        } else {
            l2_num = configuration.getL2Caches().getMaxNum();
        }

        for (ActorRef l1Cache : l1CacheActors) {
            if (randomL2Num) {
                l2_num = randomRangeRandom(configuration.getL2Caches().getMaxNum());
            }
            for (int i = 0; i < l2_num; i++) {
                this.l2CacheActors.add(system.actorOf(Cache.props(i+total_l2_caches,
                        "L2",
                        l1Cache,
                        databaseActor,
                        configuration.getL2Caches().getTimeouts())));
            }
            total_l2_caches += l2_num;
        }

        // Build clients up to maxNum for each L2 cache
        this.clientActors = new HashSet<>();
        int total_clients = 0;
        int client_num = 0;
        boolean randomClientNum = false;
        if (isUnbalanced) {
            randomClientNum = true;
        } else {
            client_num = configuration.getClients().getMaxNum();
        }

        for (ActorRef l2Cache : l2CacheActors) {
            if (randomClientNum) {
                client_num = randomRangeRandom(configuration.getClients().getMaxNum());
            }
            for (int i = 0; i < client_num; i++) {
                this.clientActors.add(system.actorOf(Client.props(i+total_clients,
                        l2Cache,
                        configuration.getClients().getTimeouts(),
                        l2CacheActors)));
            }
            total_clients += client_num;
        }
        this.master = system.actorOf(Master.props(this.l1CacheActors, this.l2CacheActors, this.clientActors), "master");
    }

    public void buildSystem(){
        if (configuration.getSystemProperty().getCustom()){
            buildCustomSystem();
        } else {
            buildAutoSystem();
        }
    }

    public void init() {
        for (ActorRef client: this.clientActors){
            // send init message to client
            Message.StartInitMsg msg = new Message.StartInitMsg();
            client.tell(msg, ActorRef.noSender());
        }
        System.out.println("Clients initialized");
        for (ActorRef l2Cache: this.l2CacheActors){
            // send init message to l1 parent
            Message.StartInitMsg msg = new Message.StartInitMsg();
            l2Cache.tell(msg, ActorRef.noSender());
        }
        System.out.println("L2 caches initialized");
        for (ActorRef l1Cache: this.l1CacheActors){
            // send init message to db
            Message.StartInitMsg msg = new Message.StartInitMsg();
            l1Cache.tell(msg, ActorRef.noSender());
        }
        System.out.println("L1 caches initialized");
    }


    private void sendWriteMsgs() {
        ActorRef[] tmpArray = this.clientActors.toArray(new ActorRef[this.clientActors.size()]);

        // generate a random number
        Random rndm = new Random();

        // this will generate a random number between 0 and
        // HashSet.size - 1
        int rndmNumber = rndm.nextInt(this.clientActors.size());
        ActorRef client = tmpArray[rndmNumber];
        Message.StartWriteMsg msg = new Message.StartWriteMsg(0, 10);
        client.tell(msg, ActorRef.noSender());
    }

    private void sendReadMsgs() {
        ActorRef[] tmpArray = this.clientActors.toArray(new ActorRef[this.clientActors.size()]);

        // generate a random number
        Random rndm = new Random();

        // this will generate a random number between 0 and
        // HashSet.size - 1
        int rndmNumber = rndm.nextInt(this.clientActors.size());
        ActorRef client = tmpArray[rndmNumber];
        Message.StartReadRequestMsg msg = new Message.StartReadRequestMsg(0);
        client.tell(msg, ActorRef.noSender());
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        DistributedCacheSystem distributedCacheSystem = new DistributedCacheSystem("config.yaml");
        String configFilePath = System.getProperty("user.dir") + distributedCacheSystem.config_file;

        System.out.println("Loading config from: " + configFilePath);

        distributedCacheSystem.parse();
        distributedCacheSystem.buildSystem();
        System.out.println("System built!");
        distributedCacheSystem.init();
        distributedCacheSystem.sendReadMsgs();
        sleep(2000);
        //distributedCacheSystem.sendWriteMsgs();
        //sleep(2000);


        //client read msg


        distributedCacheSystem.databaseActor.tell(new Message.CurrentDataMsg(), ActorRef.noSender());

        Route getClients = new HTTPRoutes().getClients(distributedCacheSystem);
        Route getL1Caches = new HTTPRoutes().getL1Caches(distributedCacheSystem);
        Route getL2Caches = new HTTPRoutes().getL2Caches(distributedCacheSystem);
        Route crashL2Caches = new HTTPRoutes().crashL2caches(distributedCacheSystem);
        Route crashL1Caches = new HTTPRoutes().crashL1caches(distributedCacheSystem);
        Route recoverL2Caches = new HTTPRoutes().recoverL2caches(distributedCacheSystem);
        Route recoverL1Caches = new HTTPRoutes().recoverL1caches(distributedCacheSystem);

        Route concat = concat(getClients, getL1Caches, getL2Caches, crashL2Caches, crashL1Caches, recoverL2Caches, recoverL1Caches);

        Http.get(distributedCacheSystem.system)
                .newServerAt("localhost", 3003)
                .bind(concat);



//        try {
//            sleep(2000);
//            System.out.println(">>> Press ENTER to exit <<<");
//            System.in.read();
//        }
//        catch (IOException ioe) {} catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        system.terminate();

    }

}
