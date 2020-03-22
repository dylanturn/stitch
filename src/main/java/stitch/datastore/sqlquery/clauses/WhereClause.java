package stitch.datastore.sqlquery.clauses;

import stitch.datastore.sqlquery.conditions.QueryConditionGroup;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WhereClause implements Serializable{

    private static final long serialVersionUID = 6555L;

    private List<QueryConditionGroup> clauseConditions = new ArrayList<>();

    public WhereClause addCondition(QueryConditionGroup clauseConditionGroup) {
        this.clauseConditions.add(clauseConditionGroup);
        return this;
    }

    public WhereClause addAllConditions(QueryConditionGroup[] queryConditionGroups) {
        this.clauseConditions.addAll(Arrays.asList(queryConditionGroups));
        return this;
    }

    public WhereClause setConditions(QueryConditionGroup[] queryConditionGroups) {
        this.clauseConditions = Arrays.asList(queryConditionGroups);
        return this;
    }

    public QueryConditionGroup[] getConditions() {
        return clauseConditions.toArray(new QueryConditionGroup[0]);
    }
}
