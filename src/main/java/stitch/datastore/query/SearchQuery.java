package stitch.datastore.query;

import java.io.Serializable;
import java.util.LinkedList;

public class SearchQuery implements Serializable {

    private static final long serialVersionUID = 5343L;

    private LinkedList<QueryCondition> queryConditions = new LinkedList<>();

    public SearchQuery(){}

    public SearchQuery addCondition(String metaKey, QueryCondition.Operator operator, String value){
        this.queryConditions.add(new QueryCondition(metaKey, operator, value));
        return this;
    }

    public SearchQuery addCondition(String metaKey, QueryCondition.Operator operator, long value){
        this.queryConditions.add(new QueryCondition(metaKey, operator, value));
        return this;
    }

    public SearchQuery addCondition(String metaKey, QueryCondition.Operator operator, int value){
        this.queryConditions.add(new QueryCondition(metaKey, operator, value));
        return this;
    }

    public SearchQuery addCondition(String metaKey, QueryCondition.Operator operator, boolean value){
        this.queryConditions.add(new QueryCondition(metaKey, operator, value));
        return this;
    }

    public QueryCondition[] getConditions(){
        return queryConditions.toArray(new QueryCondition[0]);
    }
}
