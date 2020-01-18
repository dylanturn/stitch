package stitch.amqp;

public enum AMQPPrefix {
    DATASTORE ("datastore"),
    AGGREGATOR ("aggregator");

    private final String name;
    private AMQPPrefix(String s) { name = s; }
    public boolean equalsName(String otherName) { return name.equals(otherName); }
    public String toString() { return this.name; }
}