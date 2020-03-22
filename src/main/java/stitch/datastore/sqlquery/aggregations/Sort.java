package stitch.datastore.sqlquery.aggregations;

import java.util.Arrays;
import java.util.List;

public class Sort implements QueryAggregation {

    private static final long serialVersionUID = 6564L;

    private Direction sortDirection;
    private List<String> sortFields;

    public Sort(){}
    public Sort(String[] sortFields, Direction sortDirection){
        this.sortFields = Arrays.asList(sortFields);
        this.sortDirection = sortDirection;
    }
    public Sort(String sortField, Direction sortDirection){
        this(new String[]{sortField}, sortDirection);
    }

    public Direction getSortDirection(){
        return sortDirection;
    }

    public String[] getSortFields(){
        return sortFields.toArray(new String[0]);
    }

    public Sort addSortField(String sortField){
        this.sortFields.add(sortField);
        return this;
    }

    public Sort addAllSortFields(String[] sortFields){
        this.sortFields.addAll(Arrays.asList(sortFields));
        return this;
    }

    public Sort setSortDirection(Direction sortDirection){
        this.sortDirection = sortDirection;
        return this;
    }

    public enum Direction{
        ASC,
        DEC
    }
}
