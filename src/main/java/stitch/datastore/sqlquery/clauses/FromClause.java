package stitch.datastore.sqlquery.clauses;

import stitch.datastore.sqlquery.conditions.QueryConditionGroup;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FromClause implements Serializable{

    private static final long serialVersionUID = 6554L;

    private List<QueryConditionGroup> clauseConditions = new ArrayList<>();

    public FromClause addCondition(QueryConditionGroup clauseCondition) {
        this.clauseConditions.add(clauseCondition);
        return this;
    }

    public FromClause addAllConditions(QueryConditionGroup[] clauseConditions) {
        this.clauseConditions.addAll(Arrays.asList(clauseConditions));
        return this;
    }

    public FromClause setConditions(QueryConditionGroup[] clauseConditions){
        this.clauseConditions =  Arrays.asList(clauseConditions);
        return this;
    }

    public QueryConditionGroup[] getConditions() {
        return clauseConditions.toArray(new QueryConditionGroup[0]);
    }
}
