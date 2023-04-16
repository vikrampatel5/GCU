import exceptions.DBAppException;
import model.SQLTerm;
import model.Table;
import service.AppConfigs;
import service.FileProcessor;

import java.io.*;
import java.lang.reflect.Field;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class DBApp {

    AppConfigs appConfigs = AppConfigs.getInstance();

    public static void main(String[] args) throws DBAppException {

        String strTableName = "Student";
        DBApp dbApp = new DBApp();

        //Initialize Application related Configs
        dbApp.init();

        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");

        dbApp.createTable(strTableName, "id", htblColNameType);


        //dbApp.createIndex(strTableName, new String[]{"gpa"});

        Hashtable htblColNameValue = new Hashtable();
        htblColNameValue.put("id", new Integer(2343432));
        htblColNameValue.put("name", new String("Ahmed Noor"));
        htblColNameValue.put("gpa", new Double(0.95));
        dbApp.insertIntoTable(strTableName, htblColNameValue);

        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(453455));
        htblColNameValue.put("name", new String("Ahmed Noor"));
        htblColNameValue.put("gpa", new Double(0.95));
        dbApp.insertIntoTable(strTableName, htblColNameValue);

        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(5674567));
        htblColNameValue.put("name", new String("Dalia Noor"));
        htblColNameValue.put("gpa", new Double(1.25));
        dbApp.insertIntoTable(strTableName, htblColNameValue);

        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(23498));
        htblColNameValue.put("name", new String("John Noor"));
        htblColNameValue.put("gpa", new Double(1.5));
        dbApp.insertIntoTable(strTableName, htblColNameValue);

        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(78452));
        htblColNameValue.put("name", new String("Zaky Noor"));
        htblColNameValue.put("gpa", new Double(0.88));
        dbApp.insertIntoTable(strTableName, htblColNameValue);

        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0]._strTableName = "Student";
        arrSQLTerms[0]._strColumnName = "name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue = "John Noor";
        arrSQLTerms[1]._strTableName = "Student";
        arrSQLTerms[1]._strColumnName = "gpa";
        arrSQLTerms[1]._strOperator = "=";
        arrSQLTerms[1]._objValue = new Double(1.5);
        String[] strarrOperators = new String[1];
        strarrOperators[0] = "OR";

        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
    }

    public void init() {
        appConfigs.loadConfigs("resources/DBApp.config");
    }


    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary
    // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    // htblColNameMin and htblColNameMax for passing minimum and maximum values
    // for data in the column. Key is the name of the column
    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType)
            throws DBAppException {

    }

    // following method creates an octree
    // depending on the count of column names passed.
    // If three column names are passed, create an octree.
    // If only one or two column names is passed, throw an Exception.
    public void createIndex(String strTableName,
                            String[] strarrColName) throws DBAppException {

    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
            throws DBAppException {

        String fileName = strTableName + "_part";
        Table table = new Table();

        if (htblColNameValue.get("id") == null) {
            throw new DBAppException("Primary Key can not be null");
        } else {
            table.setId((int) htblColNameValue.get("id"));
            table.setName((String) htblColNameValue.get("name"));
            table.setGpa((double) htblColNameValue.get("gpa"));
        }

        Vector<Table> data = new Vector<>();
        data.add(table);

        int partNo = 1;
        int maxLines = Integer.parseInt(appConfigs.getConfigs().getProperty("MaximumRowsCountinTablePage", "200"));

        try {
            File file = new File(fileName);
            if (file.exists()) {
                int lineCount = 0;
                try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                    while (br.readLine() != null) {
                        lineCount++;
                    }
                }

                if (lineCount > maxLines) {
                    partNo++;
                    System.out.println("Number of lines exceeds maximum limit, Creating another page.");
                }
            }

            FileProcessor.saveOrUpdateFile(strTableName + partNo, data);


            System.out.println("Serialized data is saved in info.ser");

        } catch (IOException e) {
            throw new DBAppException("Error while saving data into page.");
        }


    }

    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue)
            throws DBAppException {

        try {

            // Read objects from the file
            List<Table> list = FileProcessor.readFile(strTableName);

            // Modify objects as per your requirement
            for (Table table : list) {
                if (table.getId() == Integer.parseInt(strClusteringKeyValue)) {
                    htblColNameValue.keySet().forEach(key -> {
                        Field field;
                        try {
                            field = table.getClass().getDeclaredField(key);
                            field.set(table, htblColNameValue.get(key));
                            field.setAccessible(true);
                        } catch (NoSuchFieldException | IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }

                FileProcessor.saveOrUpdateFile(strTableName, list);

            }
        } catch (Exception e) {
            throw new DBAppException("Exception while updating row!!");
        }
    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
            throws DBAppException {

        try {
            // Read the .ser file into memory
            List<Table> objects = FileProcessor.readFile(strTableName);

            String fieldName = htblColNameValue.keySet().stream().findFirst().get();
            Object value = htblColNameValue.get(fieldName);

            // Iterate over each object and remove it if it finds the value in the search
            objects.removeIf(obj -> {
                Field field;
                try {
                    field = obj.getClass().getDeclaredField(fieldName);
                    field.setAccessible(true);
                    if (field.get(obj) == value) {
                        return true;
                    }
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
                return false;
            });

            FileProcessor.saveOrUpdateFile(strTableName, objects);

        } catch (Exception e) {
            throw new DBAppException("Exception while updating the row");
        }

    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators)
            throws DBAppException {

        Vector<Table> obj = new Vector<>();

        String sql = generateSql(arrSQLTerms, strarrOperators);

        try (FileInputStream fis = new FileInputStream("data.ser");
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            obj = (Vector<Table>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new DBAppException("Exception while reading data from table!!");
        }

        return obj.iterator();
    }

    private String generateSql(SQLTerm[] arrSQLTerms,
                               String[] strarrOperators) {
        String sqlStatement = "select * from ";
        String tableName = arrSQLTerms[0]._strTableName;

        int index = 0;
        StringBuilder clause = new StringBuilder();
        for (SQLTerm arrSQLTerm : arrSQLTerms) {
            clause.append(arrSQLTerm._strColumnName)
                    .append(arrSQLTerm._strOperator)
                    .append(arrSQLTerm._objValue);
            if (index < arrSQLTerms.length) {
                clause.append(" ")
                        .append(arrSQLTerm._strOperator)
                        .append(" ");
            }
            clause.append(" ").append(strarrOperators[index]).append(" ");
            index++;
        }

        sqlStatement = sqlStatement + tableName + " where " + clause;

        return sqlStatement;
    }

}
