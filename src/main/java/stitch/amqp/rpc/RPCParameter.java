package stitch.amqp.rpc;

public class RPCParameter {
    private String paramName;
    private Class paramClass;
    private Object paramValue;

    public RPCParameter setName(String paramName){
        this.paramName = paramName;
        return this;
    }
    public RPCParameter setClass(Class paramClass){
        this.paramClass = paramClass;
        return this;
    }
    public RPCParameter setParamValue(Object object) {
        this.paramValue = paramValue;
        return this;
    }

    public String getParamName() {
        return paramName;
    }
    public Class getParamClass() {
        return paramClass;
    }
    public Object getParamValue() {
        return paramValue;
    }
}
