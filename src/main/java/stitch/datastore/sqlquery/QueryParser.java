package stitch.datastore.sqlquery;

import org.apache.commons.lang3.math.NumberUtils;
import stitch.datastore.sqlquery.clauses.FromClause;
import stitch.datastore.sqlquery.clauses.SelectClause;
import stitch.datastore.sqlquery.clauses.WhereClause;
import stitch.datastore.sqlquery.conditions.QueryCondition;
import stitch.datastore.sqlquery.conditions.QueryConditionGroup;
import stitch.datastore.sqlquery.conditions.QueryConditionGroupType;
import stitch.datastore.sqlquery.conditions.QueryConditionOperator;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public class QueryParser {

    private static final String queryPattern = "SELECT\\s(?<select>.*)\\sFROM\\s(?<from>.*)\\sWHERE\\s(?<where>.*)";

    public static SearchQuery parseQuery(String queryString) throws ParseException {

        Matcher queryMatcher = Pattern.compile(queryPattern).matcher(queryString.toUpperCase());

        if(queryMatcher.find()){

            String selectQueryString = queryMatcher.group("select");
            String fromQueryString = queryMatcher.group("from");
            String whereQueryString = queryMatcher.group("where");

            System.out.println("Found select value: " + selectQueryString);
            System.out.println("Found from value: " + fromQueryString);
            System.out.println("Found where value: " + whereQueryString);

            return new SearchQuery().setSelectClause(parseSelectClause(selectQueryString))
                    .setFromClause(parseFromClause(fromQueryString))
                    .setWhereClause(parseWhereClause(whereQueryString));

        } else {
            throw new ParseException("Failed to parse query", 0);
        }
    }

    private static SelectClause parseSelectClause(String selectClauseString) throws ParseException {
        selectClauseString = selectClauseString.replaceAll("\\s+", "").toLowerCase();
        System.out.println(selectClauseString);
        return new SelectClause().addAllFields(selectClauseString.split(","));
    }

    private static FromClause parseFromClause(String fromClauseString) throws ParseException {
        if(fromClauseString.equals("*")){
            return new FromClause().addAllConditions(new QueryConditionGroup[]{new QueryConditionGroup(QueryConditionGroupType.OR).addCondition(new QueryCondition("*", QueryConditionOperator.EQ, "*"))});
        }
        return new FromClause().addAllConditions(parseQueryConditionsGroups(fromClauseString));
    }

    private static WhereClause parseWhereClause(String whereClauseString) throws ParseException {
        if(whereClauseString.equals("*")){
            return new WhereClause().addAllConditions(new QueryConditionGroup[]{new QueryConditionGroup(QueryConditionGroupType.OR).addCondition(new QueryCondition("*", QueryConditionOperator.EQ, "*"))});
        }
        return new WhereClause().addAllConditions(parseQueryConditionsGroups(whereClauseString));
    }

    private static QueryConditionGroup[] parseQueryConditionsGroups(String conditionsString) throws ParseException {
        List<QueryConditionGroup> queryConditionGroupList = new ArrayList<>();
        String[] conditionArray = conditionsString.toLowerCase().split(" and ");

        QueryConditionGroup conditionGroup = new QueryConditionGroup(QueryConditionGroupType.AND);
        for(String conditionString : conditionArray){
            if(conditionString.charAt(0) == '('){
                conditionString = StringUtils.stripStart(StringUtils.stripEnd(conditionString, ")"), "(");
                queryConditionGroupList.add(parseOrConditionGroup(conditionString));
            } else {
                conditionGroup.addCondition(parseCondition(conditionString));
            }
        }
        queryConditionGroupList.add(conditionGroup);

        return queryConditionGroupList.toArray(new QueryConditionGroup[0]);
    }

    private static QueryConditionGroup parseOrConditionGroup(String queryConditionGroupString) throws ParseException {
        String[] conditionArray = queryConditionGroupString.toLowerCase().split(" or ");
        QueryConditionGroup conditionGroup = new QueryConditionGroup(QueryConditionGroupType.OR);
        for(String conditionString : conditionArray){
            conditionGroup.addCondition(parseCondition(conditionString.replaceAll("\\s+", "").toLowerCase()));
        }
        return conditionGroup;
    }

    private static QueryCondition parseCondition(String condition) throws ParseException {

        if(condition.split("!=").length == 2) {
            String[] conditionArray = condition.split("!=");
            return new QueryCondition(conditionArray[0], QueryConditionOperator.NE, parseConditionValue(conditionArray[1]));
        } else if(condition.split("<=").length == 2) {
            String[] conditionArray = condition.split("<=");
            return new QueryCondition(conditionArray[0], QueryConditionOperator.LTE, parseConditionValue(conditionArray[1]));
        } else if(condition.split(">=").length == 2) {
            String[] conditionArray = condition.split(">=");
            return new QueryCondition(conditionArray[0], QueryConditionOperator.GTE, parseConditionValue(conditionArray[1]));
        } else if(condition.split("=").length == 2) {
            String[] conditionArray = condition.split("=");
            return new QueryCondition(conditionArray[0], QueryConditionOperator.EQ, parseConditionValue(conditionArray[1]));
        } else if(condition.split("<").length == 2) {
            String[] conditionArray = condition.split("<");
            return new QueryCondition(conditionArray[0], QueryConditionOperator.LT, parseConditionValue(conditionArray[1]));
        } else if(condition.split(">").length == 2) {
            String[] conditionArray = condition.split(">");
            return new QueryCondition(conditionArray[0], QueryConditionOperator.GT, parseConditionValue(conditionArray[1]));
        } else {
            throw new ParseException(String.format("Failed to parse condition: %s", condition), 0);
        }
    }

    private static Object parseConditionValue(String valueString){

        // First we check to see If we can parse it into a number.
        if(NumberUtils.isParsable(valueString)){

            // First we try to parse the float.
            try{
                return Float.parseFloat(valueString.trim());
            }catch(NumberFormatException nfe){}

            // If we can't parse a float we parse a long.
            try{
                return Long.parseLong(valueString.trim());
            }catch(NumberFormatException nfe){}

            // Otherwise we check to see if the value is true
            if(valueString.trim().toLowerCase().equals("true"))
                return true;

            // If it's not we check to see if it's false.
            if(valueString.trim().toLowerCase().equals("false"))
                return false;

        }
        // If we cant parse it into a number then we return the string.
        return valueString;
    }
}
