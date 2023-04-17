import exceptions.DBAppException;
import model.SQLTerm;
import model.Table;
import model.TableMetadata;
import service.AppConfigs;
import service.FileProcessor;

import java.io.*;
import java.lang.reflect.Field;
import java.util.*;

public class DBApp {

    AppConfigs appConfigs = AppConfigs.getInstance();
    static HashMap<String, Integer> currentPart = new HashMap<>();

    public static void main(String[] args) throws DBAppException {

        String strTableName = "Student";
        DBApp dbApp = new DBApp();

        //Initialize Application related Configs
        dbApp.init();

        tableGenerator(strTableName, dbApp);

        //dbApp.createIndex(strTableName, new String[]{"gpa"});

        tableDataGenerator(strTableName, dbApp);

        updateTableData(strTableName, dbApp);

        deleteTableData(strTableName, dbApp);

        //queryOutputGenerator(dbApp);
    }

    private static void deleteTableData(String strTableName, DBApp dbApp) throws DBAppException {

        Hashtable htblColNameValue = new Hashtable();
        htblColNameValue.put("id", new String("2343432"));

        deleteFromTable(strTableName, htblColNameValue);
    }

    private static void updateTableData(String strTableName, DBApp dbApp) throws DBAppException {
        Hashtable htblColNameValue = new Hashtable();
        htblColNameValue.put("name", new String("Ahmed Khan"));

        updateTable(strTableName, "2343432", htblColNameValue);

    }

    private static void queryOutputGenerator(DBApp dbApp) throws DBAppException {
        SQLTerm[] arrSQLTerms = new SQLTerm[2];
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

    private static void tableDataGenerator(String strTableName, DBApp dbApp) throws DBAppException {
        Hashtable htblColNameValue = new Hashtable();
        htblColNameValue.put("id", new Integer(2343432));
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
    }

    private static void tableGenerator(String strTableName, DBApp dbApp) throws DBAppException {

        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");

        Hashtable htblColNameMin = new Hashtable();
        htblColNameMin.put("id", "0");
        htblColNameMin.put("name", "A");
        htblColNameMin.put("gpa", "A");

        Hashtable htblColNameMax = new Hashtable();
        htblColNameMax.put("id", "10000");
        htblColNameMax.put("name", "ZZZZZZ");
        htblColNameMax.put("gpa", "ZZZZZZ");

        dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);

        //Initializing the default file part number for the table
        currentPart.put(strTableName, 1);
    }

    public void init() {
        appConfigs.loadConfigs("src/main/resources/DBApp.config");
    }


    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType,
                            Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax)
            throws DBAppException {

        String fileName = "src/main/resources/output/metadata.csv";

        Vector<TableMetadata> metadata = new Vector<>();

        htblColNameType.keySet().forEach(key -> {
            TableMetadata tableMetadata = new TableMetadata();
            tableMetadata.setTableName(strTableName);
            tableMetadata.setColumnName(key);
            tableMetadata.setColumnType(htblColNameType.get(key));
            tableMetadata.setIndexType("Octree");

            if (key.equalsIgnoreCase(strClusteringKeyColumn)) {
                tableMetadata.setClusteringKey(true);
                tableMetadata.setIndexType(null);
            }

            tableMetadata.setMin(htblColNameMin.get(key));
            tableMetadata.setMax(htblColNameMax.get(key));
            metadata.add(tableMetadata);
        });

        FileProcessor.saveMetaData(fileName, metadata);


    }

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

        int partNo = currentPart.get(strTableName);
        int maxLines = Integer.parseInt(appConfigs.getConfigs().getProperty("MaximumRowsCountinTablePage", "200"));

        try {
            String filePath = "src/main/resources/output/" + strTableName + partNo + ".ser";
            File file = new File(filePath);
            if (file.exists()) {
                int lineCount = 0;
                try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                    while (br.readLine() != null) {
                        lineCount++;
                    }
                }

                if (lineCount > maxLines) {
                    partNo++;
                    currentPart.put(strTableName, partNo);
                    filePath = "src/main/resources/output/" + strTableName + partNo + ".ser";
                    System.out.println("Number of lines exceeds maximum limit, Creating another page.");
                }
            }

            FileProcessor.saveOrUpdateFile(filePath, data);


            System.out.println("Serialized data is saved in info.ser");

        } catch (IOException e) {
            throw new DBAppException("Error while saving data into page.");
        }


    }

    public static void updateTable(String strTableName,
                                   String strClusteringKeyValue,
                                   Hashtable<String, Object> htblColNameValue)
            throws DBAppException {

        try {

            String path = "src/main/resources/output/";
            File folder = new File(path);
            File[] files = folder.listFiles((dir, name) -> name.startsWith(strTableName));
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        List<Table> list = FileProcessor.readFile(file.toString());
                        for (Table table : list) {
                            if (table.getId() == Integer.parseInt(strClusteringKeyValue)) {
                                System.out.println("Old Row: " + table);
                                htblColNameValue.keySet().forEach(key -> {
                                    Field field;
                                    try {
                                        field = table.getClass().getDeclaredField(key);
                                        field.set(table, htblColNameValue.get(key));
                                        field.setAccessible(true);
                                        System.out.println("Updated Row: " + table);
                                    } catch (NoSuchFieldException | IllegalAccessException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                            }
                        }
                        FileProcessor.saveOrUpdateFile(file.toString(), list);
                    }
                }


                // Modify objects as per your requirement


            }
        } catch (Exception e) {
            throw new DBAppException("Exception while updating row!!");
        }
    }

    public static void deleteFromTable(String strTableName,
                                       Hashtable<String, Object> htblColNameValue)
            throws DBAppException {

        try {

            String path = "src/main/resources/output/";
            File folder = new File(path);
            File[] files = folder.listFiles((dir, name) -> name.startsWith(strTableName));
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        List<Table> rows = FileProcessor.readFile(file.toString());
                        System.out.println("Rows Count: "+rows.size());
                            htblColNameValue.keySet().forEach(key -> {
                                String fieldName = key;
                                Object value = htblColNameValue.get(fieldName);

                                // Iterate over each object and remove it if it finds the value in the search
                                rows.removeIf(obj -> {
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
                            });
                        System.out.println("Updated Row Count: "+rows.size());
                        if(rows.size()==0){
                            if (file.delete()) {
                                System.out.println("File deleted successfully: "+file.getName());
                            } else {
                                System.out.println("Failed to delete the file: "+file.getName());
                            }
                        }
                        FileProcessor.saveOrUpdateFile(file.toString(), rows);
                    }
                }
            }



        } catch (Exception e) {
            throw new DBAppException("Exception while Deleting Data");
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
