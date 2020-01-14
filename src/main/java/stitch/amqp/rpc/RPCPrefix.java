package stitch.amqp.rpc;

public enum RPCPrefix {
    DATASTORE ("datastore"),
    AGGREGATOR ("aggregator");

    private final String name;
    private RPCPrefix(String s) { name = s; }
    public boolean equalsName(String otherName) { return name.equals(otherName); }
    public String toString() { return this.name; }
}