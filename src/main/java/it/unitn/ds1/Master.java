package it.unitn.ds1;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.*;

public class Master {
    private final String config_file;

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
        Configuration configuration = yaml.load(inputStream);
        System.out.println("Parsed config file!");
        System.out.println(configuration.getDatabase().getTimeouts().get(0).getType());
    }

    public static void main(String[] args) throws IOException{
        Master master = new Master("config.yaml");
        master.parse();
    }
}

