package stitch.datastore.query;

import java.io.Serializable;

public class QueryCondition implements Serializable {

    private static final long serialVersionUID = 6543L;

    private String metaKey;
    private Operator operator;
    private Object value;
    private Class valueClass;

    public QueryCondition(String metaKey, Operator operator, long value) {
        this.metaKey = metaKey;
        this.operator = operator;
        this.value = String.valueOf(value);
        this.valueClass = long.class;
    }

    public QueryCondition(String metaKey, Operator operator, String value) {
        this.metaKey = metaKey;
        this.operator = operator;
        this.value = value;
        this.valueClass = String.class;
    }

    public QueryCondition(String metaKey, Operator operator, boolean value) {
        this.metaKey = metaKey;
        this.operator = operator;
        this.value = String.valueOf(value);
        this.valueClass = boolean.class;
    }

    public String getMetaKey() { return metaKey; }
    public Operator getOperator() { return operator; }
    public Object getValue() { return value; }
    public Class getValueClass() { return valueClass; }

    @Override
    public String toString(){
        return String.format("%s=%s,%s", metaKey, operator, value);
    }

    public enum Operator{
        EQ,
        NE,
        LTE,
        LT,
        GTE,
        GT
    }
}


