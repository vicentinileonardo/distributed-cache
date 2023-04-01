package it.unitn.ds1;

public class TimeoutConfiguration {
    private String type;
    private int value;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public TimeoutConfiguration(){}

    public TimeoutConfiguration(String type, int value){
        this.type = type;
        this.value = value;
    }
}
