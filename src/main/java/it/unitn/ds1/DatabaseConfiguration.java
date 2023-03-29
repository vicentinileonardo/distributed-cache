package it.unitn.ds1;

import java.util.ArrayList;
import java.util.List;

public class DatabaseConfiguration {
    private List<TimeoutConfiguration> timeouts = new ArrayList<>();

    public List<TimeoutConfiguration> getTimeouts() {
        return timeouts;
    }

    public void setTimeouts(List<TimeoutConfiguration> timeouts) {
        for(TimeoutConfiguration timeout: timeouts){
            TimeoutConfiguration tmp = new TimeoutConfiguration(timeout.getType(), timeout.getValue());
            this.timeouts.add(tmp);
        }
    }

    public DatabaseConfiguration(){}

    public DatabaseConfiguration(List<TimeoutConfiguration> timeouts) {
        for (TimeoutConfiguration timeout : timeouts) {
            TimeoutConfiguration tmp = new TimeoutConfiguration(timeout.getType(), timeout.getValue());
            this.timeouts.add(tmp);
        }
    }
}
