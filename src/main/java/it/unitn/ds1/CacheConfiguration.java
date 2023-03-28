package it.unitn.ds1;

import java.util.ArrayList;

public class CacheConfiguration {
    private int maxNum;
    private ArrayList<Timeout> timeouts = new ArrayList<Timeout>();

    public int getMaxNum() {
        return maxNum;
    }

    public void setMaxNum(int maxNum) {
        this.maxNum = maxNum;
    }

    public ArrayList<Timeout> getTimeouts() {
        return timeouts;
    }

    public void setTimeouts(ArrayList<Timeout> timeouts) {
        for(Timeout timeout: timeouts){
            Timeout tmp = new Timeout(timeout.getType(), timeout.getValue());
            this.timeouts.add(tmp);
        }
    }

    public CacheConfiguration(){}

    public CacheConfiguration(int maxNum, ArrayList<Timeout> timeouts) {
        this.maxNum = maxNum;
        for (Timeout timeout : timeouts) {
            Timeout tmp = new Timeout(timeout.getType(), timeout.getValue());
            this.timeouts.add(tmp);
        }
    }
}
