package it.unitn.ds1;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.*;

public class Master {
    private final String config_file;
    private Configuration configuration;

    public Master(String config_file) {
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

    public void buildSystem(){
        boolean isUnbalanced = configuration.getSystemProperty().getUnbalanced();
        if (isUnbalanced){
            System.out.println("System is unbalanced!");
            // Build database
            // Build L1 caches up to maxNum
            // Build L2 caches up to maxNum for each L1 cache
            // Build clients up to maxNum for each L2 cache
        } else {
            System.out.println("System is balanced!");
            // Build database
            // Build maxNum L1 caches
            // Build maxNum L2 caches
            // Build maxNum clients
        }
    }

    public static void main(String[] args) throws IOException{
        Master master = new Master("config.yaml");
        master.parse();
        master.buildSystem();
    }
}

