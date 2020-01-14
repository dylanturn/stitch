package stitch.amqp.rpc;

// TODO: Stop using ResponseBytes.java ans start using this instead.
public enum RPCStatusCode {
    OK (50),
    EMPTY (60),
    ERROR (70);
    private final int value;
    private RPCStatusCode(int i) { value = i; }
    public int toInteger() { return this.value; }
}