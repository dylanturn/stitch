package stitch.datastore.sqlquery.conditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryConditionGroup implements Serializable {

    private static final long serialVersionUID = 6503L;

    private QueryConditionGroupType queryConditionGroupType;
    private List<QueryCondition> queryConditionList = new ArrayList<>();

    public QueryConditionGroup(QueryConditionGroupType queryConditionGroupType){
        this.queryConditionGroupType = queryConditionGroupType;
    }

    public QueryConditionGroupType getQueryConditionGroupType(){
        return queryConditionGroupType;
    }
    public QueryCondition[] getGroupConditions(){
        return queryConditionList.toArray(new QueryCondition[0]);
    }

    public QueryConditionGroup addCondition(QueryCondition queryCondition){
        this.queryConditionList.add(queryCondition);
        return this;
    }

    public QueryConditionGroup addAllConditions(QueryCondition[] queryCondition){
        this.queryConditionList.addAll(Arrays.asList(queryCondition));
        return this;
    }
}


