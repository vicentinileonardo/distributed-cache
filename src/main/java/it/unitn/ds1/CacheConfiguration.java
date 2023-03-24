package it.unitn.ds1;

import java.util.List;

public class CacheConfiguration {
    private int max_num;
    private List<Timeout> timeouts;

    public int getMax_num() {
        return max_num;
    }

    public void setMax_num(int max_num) {
        this.max_num = max_num;
    }

    public List<Timeout> getTimeouts() {
        return timeouts;
    }

    public void setTimeouts(List<Timeout> timeouts) {
        this.timeouts = timeouts;
    }

}
