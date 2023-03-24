package it.unitn.ds1;

import akka.actor.ActorRef;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class Master {
    private final String config_file;
//    private ActorRef database;
//    private Set<ActorRef> L1_caches = new HashSet<>();
//    private Set<ActorRef> L2_caches = new HashSet<>();
//    private Set<ActorRef> clients = new HashSet<>();

    public Master(String config_file) {
        this.config_file = config_file;
    }

    public void parse() throws IOException {
        String configFilePath = System.getProperty("user.dir") + "/config.yaml";

        System.out.println("Loading config from: " + configFilePath);


        Yaml yaml = new Yaml(new Constructor(Configuration.class));
        System.out.println("New yaml object!");
        InputStream inputStream = new FileInputStream(new File(configFilePath));

        System.out.println("Read yaml file!");
        Configuration configuration = yaml.load(inputStream); //TODO: Fix parsing, error on DatabaseConfig constructor
        System.out.println("Parsed config file!");
        System.out.println(configuration);
    }

    public static void main(String[] args) throws IOException{
        Master master = new Master("config.yaml");
        master.parse();
    }
}

