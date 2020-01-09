package stitch.amqp.rpc;

public class RPCObject {
    private RPCPrefix prefix;
    private String id;
    private int callRecordQueueLength = 100;
    private RPCStats rpcStats;

    public RPCObject(String prefix, String id){
        this(RPCPrefix.valueOf(prefix), id);
    }

    public RPCObject(RPCPrefix prefix, String id){
        this.prefix = prefix;
        this.id = id;
        this.rpcStats = new RPCStats(callRecordQueueLength);
    }

    public RPCPrefix getPrefix(){
        return this.prefix;
    }
    public String getPrefixString(){
        return this.prefix.toString();
    }
    public String getId(){
        return this.id;
    }
    public String getRouteKey(){
        return String.format("%s_%s", this.getPrefixString(), this.getId());
    }

    public enum RPCPrefix {
        DATASTORE ("datastore"),
        AGGREGATOR ("aggregator");

        private final String name;
        private RPCPrefix(String s) { name = s; }
        public boolean equalsName(String otherName) { return name.equals(otherName); }
        public String toString() { return this.name; }
    }

    // TODO: Stop using ResponseBytes.java ans start using this instead.
    public enum RPCResponseCode {
        OK (50),
        EMPTY (60),
        ERROR (70);
        private final int value;
        private RPCResponseCode(int i) { value = i; }
        public int toInteger() { return this.value; }
    }
}
