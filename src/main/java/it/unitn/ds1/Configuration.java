package it.unitn.ds1;

public class Configuration {
    private SystemProperty systemProperty;
    private ClientConfiguration clients;
    private CacheConfiguration L1Caches;
    private CacheConfiguration L2Caches;
    private DatabaseConfiguration database;

    public SystemProperty getSystemProperty() {
        return systemProperty;
    }

    public void setSystemProperty(SystemProperty systemProperty) {
        this.systemProperty = systemProperty;
    }

    public ClientConfiguration getClients() {
        return clients;
    }

    public void setClients(ClientConfiguration clients) {
        this.clients = clients;
    }

    public CacheConfiguration getL1Caches() {
        return L1Caches;
    }

    public void setL1Caches(CacheConfiguration l1Caches) {
        L1Caches = l1Caches;
    }

    public CacheConfiguration getL2Caches() {
        return L2Caches;
    }

    public void setL2Caches(CacheConfiguration l2Caches) {
        L2Caches = l2Caches;
    }

    public DatabaseConfiguration getDatabase() {
        return database;
    }

    public void setDatabase(DatabaseConfiguration database) {
        this.database = database;
    }

    public Configuration(){}

    public Configuration(ClientConfiguration clients,
                         CacheConfiguration L1Caches,
                         CacheConfiguration L2Caches,
                         DatabaseConfiguration database){
        this.clients = clients;
        this.L1Caches = L1Caches;
        this.L2Caches = L2Caches;
        this.database = database;
    }

}
