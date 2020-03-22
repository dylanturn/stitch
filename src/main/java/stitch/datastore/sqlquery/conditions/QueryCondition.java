package stitch.datastore.sqlquery.conditions;

import java.io.Serializable;

public class QueryCondition implements Serializable {

    private static final long serialVersionUID = 6543L;

    private String conditionField;
    private QueryConditionOperator queryConditionOperator;
    private Object conditionValue;

    public QueryCondition(){}
    public QueryCondition(String conditionField, QueryConditionOperator queryConditionOperator, Object conditionValue){
        this.conditionField = conditionField;
        this.queryConditionOperator = queryConditionOperator;
        this.conditionValue = conditionValue;
    }

    public String getConditionField(){
        return conditionField;
    }

    public QueryConditionOperator getQueryConditionOperator() {
        return queryConditionOperator;
    }

    public Object getConditionValue() {
        return conditionValue;
    }

    public QueryCondition setConditionField(String conditionField){
        this.conditionField = conditionField;
        return this;
    }

    public QueryCondition setConditionOperator(QueryConditionOperator queryConditionOperator){
        this.queryConditionOperator = queryConditionOperator;
        return this;
    }

    public QueryCondition setConditionValue(Object conditionValue){
        this.conditionValue = conditionValue;
        return this;
    }
}
