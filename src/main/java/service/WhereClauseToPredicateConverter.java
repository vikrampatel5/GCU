package service;

import exceptions.DBAppException;
import model.Table;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WhereClauseToPredicateConverter {

    public static Predicate<Table> convertWhereClauseToPredicate(String whereClause) throws DBAppException {
        String[] conditions = whereClause.split("( AND | OR )");
        List<Predicate<Table>> predicates = new ArrayList<>();
        for (String condition : conditions) {
            String[] parts = condition.split(" ");
            String field = parts[0];
            String operator = parts[1];
            String value = "";
            if(condition.contains("\'"))
                 value = condition.substring(condition.indexOf('\''), condition.lastIndexOf('\'')+1);
            else value = parts[2].replace("'", "");
            Predicate<Table> predicate = createPredicate(field, operator, value.replaceAll("'",""));
            predicates.add(predicate);
        }
        return createCompoundPredicate(predicates, whereClause);
    }

    private static Predicate<Table> createPredicate(String field, String operator, String value) throws DBAppException {
        try {
            if ("=".equals(operator)) {
                return p -> getValue(p, field).toString().equalsIgnoreCase(value);
            } else if (">".equals(operator)) {
                return p -> (double)getValue(p, field) > Double.parseDouble(value);
            } else if ("<".equals(operator)) {
                return p -> (double)getValue(p, field) < Double.parseDouble(value);
            } else if (">=".equals(operator)) {
                return p -> (double)getValue(p, field) >= Double.parseDouble(value);
            } else if ("<=".equals(operator)) {
                return p -> (double)getValue(p, field) <= Double.parseDouble(value);
            } else if ("!=".equals(operator)) {
                return p -> !getValue(p, field).equals(value);
            }
            throw new IllegalArgumentException("Unknown operator: " + operator);
        } catch (Exception e) {
            throw  new DBAppException(e.getMessage());
        }
    }

    private static Predicate<Table> createCompoundPredicate(List<Predicate<Table>> predicates, String whereClause) {
        Predicate<Table> compoundPredicate = predicates.get(0);
        int currentPredicateIndex = 0;
        String[] conditions = splitText(whereClause);
        for (int i = 3; i < conditions.length; i++) {
            String condition = conditions[i];
            if (condition.equalsIgnoreCase("AND") ) {
                currentPredicateIndex++;
                Predicate<Table> nextPredicate = predicates.get(currentPredicateIndex);
                if (compoundPredicate == null) {
                    compoundPredicate = nextPredicate;
                } else {
                    compoundPredicate = compoundPredicate.and(nextPredicate);
                }
            } else if (condition.equalsIgnoreCase("OR")) {
                currentPredicateIndex++;
                Predicate<Table> nextPredicate = predicates.get(currentPredicateIndex);
                if (compoundPredicate == null) {
                    compoundPredicate = nextPredicate;
                } else {
                    compoundPredicate = compoundPredicate.or(nextPredicate);
                }
            }
        }
        return compoundPredicate;
    }

    public static String[] splitText(String text) {
        List<String> parts = new ArrayList<>();
        Pattern pattern = Pattern.compile("'(.*?)'|\\S+");
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            String part = matcher.group();
            if (part.matches("'(.*?)'")) {
                parts.add(part);
            } else {
                String[] subParts = part.split("\\s+");
                parts.addAll(Arrays.asList(subParts));
            }
        }
        return parts.toArray(new String[0]);
    }

    public static Vector<Table> filterTables(List<Table> Tables, Predicate<Table> predicate) {
        Vector<Table> filteredTables = new Vector<>();
        for (Table Table : Tables) {
            if (predicate.test(Table)) {
                filteredTables.add(Table);
            }
        }
        return filteredTables;
    }

    private static Object getValue(Table Table, String colName) {
        try{
            Field field = Table.getClass().getDeclaredField(colName);
            field.setAccessible(true);
            return field.get(Table);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        return null;
    }
}