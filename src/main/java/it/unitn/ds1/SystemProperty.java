package it.unitn.ds1;

public class SystemProperty {
    private boolean unbalanced;

    public boolean getUnbalanced(){
        return this.unbalanced;
    }

    public void setUnbalanced(boolean unbalance){
        this.unbalanced = unbalance;
    }

    public SystemProperty(){}

    public SystemProperty(boolean unbalance){
        this.unbalanced = unbalance;
    }
}
