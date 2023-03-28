package it.unitn.ds1;

import java.util.ArrayList;

public class DatabaseConfiguration {
    private ArrayList<Timeout> timeouts = new ArrayList<Timeout>();

    public ArrayList<Timeout> getTimeouts() {
        return timeouts;
    }

    public void setTimeouts(ArrayList<Timeout> timeouts) {
        for(Timeout timeout: timeouts){
            Timeout tmp = new Timeout(timeout.getType(), timeout.getValue());
            this.timeouts.add(tmp);
        }
    }

    public DatabaseConfiguration(){}

    public DatabaseConfiguration(ArrayList<Timeout> timeouts) {
        for (Timeout timeout : timeouts) {
            Timeout tmp = new Timeout(timeout.getType(), timeout.getValue());
            this.timeouts.add(tmp);
        }
    }
}
