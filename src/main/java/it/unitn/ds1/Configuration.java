package it.unitn.ds1;

import java.util.List;

public class Configuration {
    private boolean unbalanced;
    private List<ClientConfiguration> clients;
    public List<CacheConfiguration> L1_caches;
    public List<CacheConfiguration> L2_caches;
    private DatabaseConfiguration database;

    public boolean getUnbalanced(){
        return this.unbalanced;
    }

    public void setUnbalanced(boolean unbalance){
        this.unbalanced = unbalance;
    }


    public List<ClientConfiguration> getClients() {
        return clients;
    }

    public void setClients(List<ClientConfiguration> clients) {
        this.clients = clients;
    }


    public List<CacheConfiguration> getL1_caches() {
        return L1_caches;
    }

    public void setL1_caches(List<CacheConfiguration> l1_caches) {
        L1_caches = l1_caches;
    }


    public List<CacheConfiguration> getL2_caches() {
        return L2_caches;
    }

    public void setL2_caches(List<CacheConfiguration> l2_caches) {
        L2_caches = l2_caches;
    }


    public DatabaseConfiguration getDatabase() {
        return database;
    }

    public void setDatabase(DatabaseConfiguration database) {
        this.database = database;
    }
}
