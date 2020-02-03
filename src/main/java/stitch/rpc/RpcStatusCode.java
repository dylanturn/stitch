package stitch.rpc;

// TODO: Stop using ResponseBytes.java ans start using this instead.
public enum RpcStatusCode {
    OK (200),
    EMPTY (204),
    MISSING (404),
    SERVER_ERROR(500),
    NOT_IMPLEMENTED(501),
    NET_READ_ERROR(598),
    NET_CONNECT_ERROR(599);
    private final int value;
    private RpcStatusCode(int i) { value = i; }
    public int toInteger() { return this.value; }
}