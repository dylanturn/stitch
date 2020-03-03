package stitch.util.configuration.item;

import java.io.Serializable;


public enum ConfigItemType implements Serializable {
    DATASTORE ("datastore"),
    AGGREGATOR ("aggregator"),
    TRANSPORT ("transport"),
    CONFIGURATION ("configuration");
    private final String value;
    private ConfigItemType(String s) { value = s; }
    public boolean equalsName(String otherName) { return value.equals(otherName); }
    public String toString() { return this.value; }
}