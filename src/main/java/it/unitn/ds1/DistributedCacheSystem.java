package it.unitn.ds1;

import akka.actor.Actor;
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

    private ActorRef crashedActor;

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
                    "l1_cache_"+i));
        }

        // Build L2 caches up to maxNum for each L1 cache
        this.l2CacheActors = new HashSet<>();
        int totalL2Caches = 0;
        int l2Num;
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
                        "l2_cache_"+(i+totalL2Caches)));
            }
            totalL2Caches += l2Num;
        }

        // Build clients up to maxNum for each L2 cache
        this.clientActors = new HashSet<>();
        int totalClients = 0;
        int clientNum;

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
                        "client_"+(i+totalClients)));
            }
            totalClients += clientNum;
        }
        System.out.println("Client " + (totalClients == totalClientsNum) );

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
                    "l1_cache_"+i));
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
                        "l2_cache_"+(i+total_l2_caches)));
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
                        "client_"+(i+total_clients)));
            }
            total_clients += client_num;
        }
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
            StartInitMsg msg = new StartInitMsg();
            client.tell(msg, ActorRef.noSender());
        }
        System.out.println("Clients initialized");
        for (ActorRef l2Cache: this.l2CacheActors){
            // send init message to l1 parent
            StartInitMsg msg = new StartInitMsg();
            l2Cache.tell(msg, ActorRef.noSender());
        }
        System.out.println("L2 caches initialized");
        for (ActorRef l1Cache: this.l1CacheActors){
            // send init message to db
            StartInitMsg msg = new StartInitMsg();
            l1Cache.tell(msg, ActorRef.noSender());
        }
        System.out.println("L1 caches initialized");
    }


    private ActorRef getRandomActor(HashSet<ActorRef> actors){
        ActorRef[] tmpArray = actors.toArray(new ActorRef[actors.size()]);

        // generate a random number
        Random rnd = new Random();

        // this will generate a random number between 0 and
        // HashSet.size - 1
        int rndNumber = rnd.nextInt(actors.size());
        return tmpArray[rndNumber];
    }

    private ActorRef findActor(String actorId, HashSet<ActorRef> actors){
        ActorRef foundActor = null;
        for(ActorRef actor : actors){
            if (actor.path().name().contains(actorId)){
                foundActor  = actor;
                break;
            }
        }
        if (foundActor == null){
            throw new NoSuchElementException();
        }

        return foundActor;
    }

    // ----------SEND READ REQUESTS----------
    private void sendReadRequestMsg(int key) {
        ActorRef client = getRandomActor(this.clientActors);
        StartReadMsg msg = new StartReadMsg(key);
        client.tell(msg, ActorRef.noSender());
    }

    private void sendReadRequestMsg(int key, String clientId) {
        StartReadMsg msg = new StartReadMsg(key);
        ActorRef client = findActor(clientId, this.clientActors);
        client.tell(msg, ActorRef.noSender());
    }

    private void sendCriticalReadMsg(int key) {
        ActorRef client = getRandomActor(this.clientActors);
        StartCriticalReadMsg msg = new StartCriticalReadMsg(key);
        client.tell(msg, ActorRef.noSender());
    }

    private void sendCriticalReadMsg(int key, String clientId) {
        StartCriticalReadMsg msg = new StartCriticalReadMsg(key);
        ActorRef client = findActor(clientId, this.clientActors);
        client.tell(msg, ActorRef.noSender());
    }

    // ----------SEND WRITE REQUESTS----------
    private void sendWriteMsg(int key, int value) {
        ActorRef client = getRandomActor(this.clientActors);
        StartWriteMsg msg = new StartWriteMsg(key, value);
        client.tell(msg, ActorRef.noSender());
    }

    private void sendWriteMsg(int key, int value, String clientId) {
        StartWriteMsg msg = new StartWriteMsg(key, value);
        ActorRef client = findActor(clientId, this.clientActors);
        client.tell(msg, ActorRef.noSender());
    }

//    private void sendCriticalWriteMsg(int key, int value) {
//        ActorRef client = getRandomClient();
//        StartCriticalWriteMsg msg = new StartCriticalWriteMsg(key, value);
//        client.tell(msg, ActorRef.noSender());
//    }
//
//    private void sendCriticalWriteMsg(int key, int value, String clientId) {
//        StartCriticalWriteMsg msg = new StartCriticalWriteMsg(key, value);
//        ActorRef client = findActor(clientId, this.clientActors);
//        client.tell(msg, ActorRef.noSender());
//    }

    // ----------SEND INFO REQUESTS----------

    private void getCurrentDBData(){
        this.databaseActor.tell(new CurrentDataMsg(), ActorRef.noSender());
    }

    private void getActorCurrentInfo(String actorId, String type){
        ActorRef actor = null;
        HashSet<ActorRef> searchList = new HashSet<>();
        switch (type){
            case "client":{
                searchList = this.clientActors;
            }
            case "L1": {
                searchList = this.l1CacheActors;
            }
            case "L2": {
                searchList = this.l2CacheActors;
            }
        }

        if (searchList.isEmpty()){
            throw new IllegalArgumentException("Unknown type");
        }

        actor = findActor(actorId, searchList);
        actor.tell(new InfoMsg(), ActorRef.noSender());
    }

    // ----------SEND CRASH MESSAGES----------
    private void crashActor(){
        ActorRef actor = getRandomActor(this.l1CacheActors);
        CrashMsg msg = new CrashMsg();
        actor.tell(msg, ActorRef.noSender());
        this.crashedActor = actor;
    }

    private void crashActor(String type){
        ActorRef actor;
        if (type.equals("L1")){
            actor = getRandomActor(this.l1CacheActors);
        } else if (type.equals("L2")){
            actor = getRandomActor(this.l1CacheActors);
        } else {
            throw new IllegalArgumentException("Unknown actor type");
        }
        CrashMsg msg = new CrashMsg();
        actor.tell(msg, ActorRef.noSender());
        this.crashedActor = actor;
    }

    private void crashActor(String type, String actorId){
        ActorRef actor;
        if (type.equals("L1")){
            actor = findActor(actorId, this.l1CacheActors);
        } else if (type.equals("L2")){
            actor = findActor(actorId, this.l2CacheActors);
        } else {
            throw new IllegalArgumentException("Unknown actor type");
        }

        CrashMsg msg = new CrashMsg();
        actor.tell(msg, ActorRef.noSender());
        this.crashedActor = actor;
    }

    private void recover(){
        this.crashedActor.tell(new RecoverMsg(), ActorRef.noSender());
        this.crashedActor = null;
    }

    private void recover(String type, String actorId){
        ActorRef actor;
        if (type.equals("L1")){
            actor = findActor(actorId, this.l1CacheActors);
        } else if (type.equals("L2")){
            actor = findActor(actorId, this.l2CacheActors);
        } else {
            throw new IllegalArgumentException("Unknown actor type");
        }
        actor.tell(new RecoverMsg(), ActorRef.noSender());
        this.crashedActor = null;
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
        distributedCacheSystem.sendWriteMsg(0, 10);
        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        }
        catch (IOException ioe){
            ioe.printStackTrace();
        }

        distributedCacheSystem.system.terminate();

    }

}
