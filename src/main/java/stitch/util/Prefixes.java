package stitch.util;

public enum  Prefixes {
    DATASTORE ("datastore"),
    AGGREGATOR ("aggregator");

    private final String name;

    private Prefixes(String s) {
        name = s;
    }

    public boolean equalsName(String otherName) {
        // (otherName == null) check is not needed because name.equals(null) returns false
        return name.equals(otherName);
    }

    public String toString() {
        return this.name;
    }
}
