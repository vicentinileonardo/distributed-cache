package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import it.unitn.ds1.Message.*;

import java.io.*;
import java.util.*;

import static java.lang.Thread.sleep;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

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
        this.system.logConfiguration();
        System.out.println("Custom system creation!");
        // Build database
        this.databaseActor = system.actorOf(Database.props(0, configuration.getDatabase().getTimeouts()),
                "database");

        // Build L1 caches up to maxNum

        this.l1CacheActors = new HashSet<>();
        int totalL1Num = configuration.getL1Caches().getCustomNum();
        for (int i = 0; i < totalL1Num; i++) {
            this.l1CacheActors.add(system.actorOf(Cache.props(i,
                    "L1",
                    databaseActor,
                    configuration.getL1Caches().getTimeouts()),
                    "l1_cache_"+String.valueOf(i)));
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
                        configuration.getL2Caches().getTimeouts()),
                        "l2_cache_"+String.valueOf(i+totalL2Caches)));
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
                        l2CacheActors),
                        "client_"+String.valueOf(i+totalClients)));
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
        this.databaseActor = system.actorOf(Database.props(0, configuration.getDatabase().getTimeouts()),
                "database");

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
                    configuration.getL1Caches().getTimeouts()),
                    "l1_cache_"+String.valueOf(i)));;
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
                        configuration.getL2Caches().getTimeouts()),
                        "l2_cache_"+String.valueOf(i+total_l2_caches)));
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
                        l2CacheActors),
                        "client_"+String.valueOf(i+total_clients)));
            }
            total_clients += client_num;
        }
        this.master = system.actorOf(Master.props(this.l1CacheActors, this.l2CacheActors, this.clientActors),
                "master");
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
        Random rnd = new Random();

        // this will generate a random number between 0 and
        // HashSet.size - 1
        int rndNumber = rnd.nextInt(this.clientActors.size());
        ActorRef client = tmpArray[rndNumber];
        Message.StartWriteMsg msg = new Message.StartWriteMsg(0, 10);
        client.tell(msg, ActorRef.noSender());
    }

    public static void main(String[] args) throws IOException {
        // System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("logs/log.txt"))));
        DistributedCacheSystem distributedCacheSystem = new DistributedCacheSystem("config.yaml");
        String configFilePath = System.getProperty("user.dir") + distributedCacheSystem.config_file;

        System.out.println("Loading config from: " + configFilePath);

        distributedCacheSystem.parse();
        distributedCacheSystem.buildSystem();
        System.out.println("System built!");
        distributedCacheSystem.init();
        distributedCacheSystem.sendWriteMsgs();
        try {
            sleep(2000);
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        }
        catch (IOException ioe) {} catch (InterruptedException e) {
            e.printStackTrace();
        }
        distributedCacheSystem.databaseActor.tell(new Message.CurrentDataMsg(), ActorRef.noSender());
        distributedCacheSystem.system.terminate();

    }

}
