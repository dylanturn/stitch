package stitch.util.properties;

public enum StitchType {
    DATASTORE ("datastore"),
    AGGREGATOR ("aggregator"),
    TRANSPORT ("transport"),
    CONFIGURATION ("configuration");
    private final String name;
    private StitchType(String s) { name = s; }
    public boolean equalsName(String otherName) { return name.equals(otherName); }
    public String toString() { return this.name; }
}