package it.unitn.ds1;

public class SystemProperty {
    private boolean unbalanced;
    private boolean custom;

    public boolean getUnbalanced(){
        return this.unbalanced;
    }

    public void setUnbalanced(boolean unbalance){
        this.unbalanced = unbalance;
    }

    public boolean getCustom() {
        return custom;
    }

    public void setCustom(boolean custom) {
        this.custom = custom;
    }

    public SystemProperty(){}

    public SystemProperty(boolean unbalance){
        this.unbalanced = unbalance;
    }
}
