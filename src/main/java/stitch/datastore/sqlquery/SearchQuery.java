package stitch.datastore.sqlquery;

import stitch.datastore.sqlquery.aggregations.QueryAggregation;
import stitch.datastore.sqlquery.clauses.FromClause;
import stitch.datastore.sqlquery.clauses.SelectClause;
import stitch.datastore.sqlquery.clauses.WhereClause;
import stitch.datastore.sqlquery.conditions.QueryConditionGroup;

import java.io.Serializable;
import java.util.LinkedList;

public class SearchQuery implements Serializable {

    private static final long serialVersionUID = 5343L;

    private SelectClause selectClause = new SelectClause();
    private FromClause fromClause = new FromClause();
    private WhereClause whereClause = new WhereClause();
    private LinkedList<QueryAggregation> queryAggregationList = new LinkedList<>();

    public SearchQuery(){}

    public String[] getSelectClause(){
        return selectClause.getFields();
    }

    public QueryConditionGroup[] getFromConditions(){
        return fromClause.getConditions();
    }

    public QueryConditionGroup[] getWhereConditions(){
        return whereClause.getConditions();
    }

    public LinkedList<QueryAggregation> getQueryAggregationList() {
        return queryAggregationList;
    }

    public SearchQuery setSelectClause(SelectClause selectClause){
        this.selectClause = selectClause;
        return this;
    }

    public SearchQuery setFromClause(FromClause fromClause){
        this.fromClause = fromClause;
        return this;
    }

    public SearchQuery setWhereClause(WhereClause whereClause){
        this.whereClause = whereClause;
        return this;
    }
}
