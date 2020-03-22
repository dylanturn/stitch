package stitch.datastore.sqlquery.clauses;

import stitch.datastore.sqlquery.conditions.QueryConditionGroup;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SelectClause implements Serializable {

    private static final long serialVersionUID = 6553L;

    private List<String> selectedFields = new ArrayList<>();

    public SelectClause addField(String selectedField){
        this.selectedFields.add(selectedField);
        return this;
    }

    public SelectClause addAllFields(String[] selectedFields) {
        this.selectedFields.addAll(Arrays.asList(selectedFields));
        return this;
    }

    public SelectClause setFields(String[] selectedFields) {
        this.selectedFields = Arrays.asList(selectedFields);
        return this;
    }

    public String[] getFields() {
        return selectedFields.toArray(new String[0]);
    }
}
