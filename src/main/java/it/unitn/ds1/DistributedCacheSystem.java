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

    public void buildSystem(){
        boolean isUnbalanced = configuration.getSystemProperty().getUnbalanced();
        this.system = ActorSystem.create("distributed_cache_system");

        if (isUnbalanced){
            System.out.println("System is unbalanced!");
            // Build database
            this.databaseActor = system.actorOf(Database.props(0, configuration.getDatabase().getTimeouts()));
            // Build L1 caches up to maxNum
            int l1_num = randomRangeRandom(configuration.getL1Caches().getMaxNum());
            this.l1CacheActors = new HashSet<>();
            for (int i = 0; i < l1_num; i++) {
                this.l1CacheActors.add(system.actorOf(Cache.props(i,
                        "L1",
                        databaseActor,
                        configuration.getL1Caches().getTimeouts())));
            }
            // TODO: populate L1 list of database with refs of L1 caches

            // Build L2 caches up to maxNum for each L1 cache
            this.l2CacheActors = new HashSet<>();
            for (ActorRef l1Cache : l1CacheActors) {
                int l2_num = randomRangeRandom(configuration.getL2Caches().getMaxNum());
                for (int i = 0; i < l2_num; i++) {
                    this.l2CacheActors.add(system.actorOf(Cache.props(i,
                            "L2",
                            l1Cache,
                            databaseActor,
                            configuration.getL2Caches().getTimeouts())));
                }

                // TODO: populate L2 list of L1 cache with refs of L2 caches

            }

            // Build clients up to maxNum for each L2 cache
            this.clientActors = new HashSet<>();
            for (ActorRef l2Cache : l2CacheActors) {
                int client_num = randomRangeRandom(configuration.getClients().getMaxNum());
                for (int i = 0; i < client_num; i++) {
                    this.clientActors.add(system.actorOf(Client.props(i,
                            l2Cache,
                            configuration.getClients().getTimeouts())));
                }
            }
        } else {
            System.out.println("System is balanced!");
            // Build database
            this.databaseActor = system.actorOf(Database.props(0, configuration.getDatabase().getTimeouts()));
            // Build L1 caches up to maxNum
            this.l1CacheActors = new HashSet<>();
            for (int i = 0; i < configuration.getL1Caches().getMaxNum(); i++) {
                this.l1CacheActors.add(system.actorOf(Cache.props(i,
                        "L1",
                        databaseActor,
                        configuration.getL1Caches().getTimeouts())));
            }
            // TODO: populate L1 list of database with refs of L1 caches

            // Build L2 caches up to maxNum for each L1 cache
            this.l2CacheActors = new HashSet<>();
            for (ActorRef l1Cache : l1CacheActors) {
                for (int i = 0; i < configuration.getL2Caches().getMaxNum(); i++) {
                    this.l2CacheActors.add(system.actorOf(Cache.props(i,
                            "L2",
                            l1Cache,
                            databaseActor,
                            configuration.getL2Caches().getTimeouts())));
                }

                // TODO: populate L2 list of L1 cache with refs of L2 caches

            }

            // Build clients up to maxNum for each L2 cache
            this.clientActors = new HashSet<>();
            for (ActorRef l2Cache : l2CacheActors) {
                for (int i = 0; i < configuration.getClients().getMaxNum(); i++) {
                    this.clientActors.add(system.actorOf(Client.props(i,
                            l2Cache,
                            configuration.getClients().getTimeouts())));
                }
            }
        }

        this.master = system.actorOf(Master.props(this.l1CacheActors, this.l2CacheActors, this.clientActors), "master");
    }

    public void init() throws IOException {
        for (ActorRef client: this.clientActors){
            // send init message to l2 parent
        }
        for (ActorRef l2Cache: this.l2CacheActors){
            // send init message to l1 parent
        }
        for (ActorRef l1Cache: this.l1CacheActors){
            // send init message to db
        }
    }
    public static void main(String[] args) throws IOException {

        DistributedCacheSystem distributedCacheSystem = new DistributedCacheSystem("config.yaml");
        String configFilePath = System.getProperty("user.dir") + distributedCacheSystem.config_file;

        System.out.println("Loading config from: " + configFilePath);

        distributedCacheSystem.parse();
        distributedCacheSystem.buildSystem();
        System.out.println("System built!");
        distributedCacheSystem.init();
        distributedCacheSystem.system.terminate();

        // create the database
        // final ActorRef database = system.actorOf(Database.props(0, ), "database");

        //read the YAML config file and create the caches
        // final

        //create the clients
//        List<ActorRef> group = new ArrayList<>();
//        for (int i = 0; i < N_CLIENTS; i++) {
//            group.add(system.actorOf(Client.props(i), "client" + i));
//        }


        //examples
        //read requests msg to the database
//        ReadRequestMsg readRequestMsg = new ReadRequestMsg(4, 0);
//        database.tell(readRequestMsg, ActorRef.noSender());
//
//        //write requests msg to the database
//        WriteRequestMsg writeRequestMsg = new WriteRequestMsg(5, 5, 3);
//        database.tell(writeRequestMsg, ActorRef.noSender());
//
//        //current database state
//        CurrentDataMsg currentDataMsg = new CurrentDataMsg();
//        database.tell(currentDataMsg, ActorRef.noSender());




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
