package it.unitn.ds1;

import java.util.ArrayList;
import java.util.List;

public class ClientConfiguration {
    private int maxNum;

    private int customNum;

    private List<TimeoutConfiguration> timeouts = new ArrayList<>();

    public int getMaxNum() {
        return maxNum;
    }

    public void setMaxNum(int maxNum) {
        this.maxNum = maxNum;
    }

    public int getCustomNum() {
        return customNum;
    }

    public void setCustomNum(int customNum) {
        this.customNum = customNum;
    }

    public List<TimeoutConfiguration> getTimeouts() {
        return timeouts;
    }

    public void setTimeouts(List<TimeoutConfiguration> timeouts) {
        for(TimeoutConfiguration timeout: timeouts){
            TimeoutConfiguration tmp = new TimeoutConfiguration(timeout.getType(), timeout.getValue());
            this.timeouts.add(tmp);
        }
    }

    public ClientConfiguration(){}

    public ClientConfiguration(int maxNum, List<TimeoutConfiguration> timeouts){
        this.maxNum = maxNum;
        for(TimeoutConfiguration timeout: timeouts){
            TimeoutConfiguration tmp = new TimeoutConfiguration(timeout.getType(), timeout.getValue());
            this.timeouts.add(tmp);
        }
    }

}
