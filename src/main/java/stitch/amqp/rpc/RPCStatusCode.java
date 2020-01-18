package stitch.amqp.rpc;

// TODO: Stop using ResponseBytes.java ans start using this instead.
public enum RPCStatusCode {
    OK (200),
    EMPTY (204),
    MISSING (404),
    ERROR (500);
    private final int value;
    private RPCStatusCode(int i) { value = i; }
    public int toInteger() { return this.value; }
}