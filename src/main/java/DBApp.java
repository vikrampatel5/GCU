import exceptions.DBAppException;
import model.SQLTerm;
import model.Table;
import model.TableMetadata;
import service.AppConfigs;
import service.FileProcessor;
import service.Octree;
import service.Point;

import java.io.*;
import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.*;

public class DBApp {

    AppConfigs appConfigs = AppConfigs.getInstance();
    static HashMap<String, Integer> currentPart = new HashMap<>();

    static Octree<String> octree;

    static HashMap<String, Point> valueIndex = new HashMap<>();

    Map<String, TableMetadata> indexColsMetadata = new HashMap<>();

    public static void main(String[] args) throws DBAppException {

        String strTableName = "Student";
        DBApp dbApp = new DBApp();

        //Initialize Application related Configs
        dbApp.init();

        tableGenerator(strTableName, dbApp);

        dbApp.createIndex(strTableName, new String[]{"id","name", "gpa"});

        tableDataGenerator(strTableName, dbApp);

        updateTableData(strTableName, dbApp);

        deleteTableData(strTableName, dbApp);

        queryOutputGenerator(dbApp);
    }

    private static void deleteTableData(String strTableName, DBApp dbApp) throws DBAppException {

        //Data to be deleted
        Hashtable htblColNameValue = new Hashtable();
        htblColNameValue.put("id", new String("78452"));

        deleteFromTable(strTableName, htblColNameValue);
    }

    private static void updateTableData(String strTableName, DBApp dbApp) throws DBAppException {

        //Data to be updated
        Hashtable htblColNameValue = new Hashtable();
        htblColNameValue.put("name", new String("Ahmed Khan"));

        //ClusteringKey (id) value to be used to find and update the value.
        String clusteringKeyValue = "2343432";
        updateTable(strTableName, clusteringKeyValue, htblColNameValue);

    }

    private static void queryOutputGenerator(DBApp dbApp) throws DBAppException {
        SQLTerm[] arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "Student";
        arrSQLTerms[0]._strColumnName = "name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue = "John Noor";

        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strTableName = "Student";
        arrSQLTerms[1]._strColumnName = "gpa";
        arrSQLTerms[1]._strOperator = "=";
        arrSQLTerms[1]._objValue = new Double(1.5);
        String[] strarrOperators = new String[1];
        strarrOperators[0] = "OR";

        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);

        while(resultSet.hasNext()) {
            System.out.println(resultSet.next());
        }
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

    public void init()  {
        appConfigs.loadConfigs("src/main/resources/DBApp.config");
        octree = FileProcessor.loadOctreeFromFile();
        FileProcessor.deleteExistingData("src/main/resources/output");
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

        try{
            if(octree == null){
                if(strarrColName.length == 3 ){
                    octree = new Octree<>(0,0,0, 7, 7, 7);
                }else{
                    throw new DBAppException(MessageFormat.format("Only {0} column names are provided. Please provide 3 column names", Arrays.stream(strarrColName).count()));
                }
            }

            Vector<TableMetadata> tableMetadata = FileProcessor.readMetadata("src/main/resources/output/metadata.csv");

            for(String indexColName : strarrColName){
                Optional<TableMetadata> matchedMetadata = tableMetadata.stream().filter(m -> m.getColumnName().equalsIgnoreCase(indexColName)).findFirst();
                indexColsMetadata.put(indexColName,matchedMetadata.get());
            }

        }catch (Exception e){
            throw new DBAppException("Error while creating index: "+e.getMessage());
        }

    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
            throws DBAppException {

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
                int rowCount = FileProcessor.readFile(file.toString()).size();

                if (rowCount > maxLines) {
                    partNo++;
                    currentPart.put(strTableName, partNo);
                    filePath = "src/main/resources/output/" + strTableName + partNo + ".ser";
                    System.out.println("Number of lines exceeds maximum limit, Creating another page.");
                }
            }

            FileProcessor.saveOrUpdateFile(filePath, data, true);

            //Add value to index
            String finalFilePath = filePath;
            htblColNameValue.keySet().forEach(key -> {
                if(indexColsMetadata.containsKey(key)){
                    addValueToIndex(strTableName, key+"#"+htblColNameValue.get(key), finalFilePath);
                }
            });


            System.out.println("Serialized data is saved in info.ser");

        } catch (IOException | ClassNotFoundException e) {
            throw new DBAppException("Error while saving data into page.");
        }


    }

    private static void addValueToIndex(String strTableName, String indexName, String refFilePath) {
        boolean pointFound = false;
        String path = "src/main/resources/output/";
        String octreeFilePath = path+"Octree"+strTableName+".ser";
        for(int x=0; x<=7; x++){
            for(int y=0; y<=7; y++){
                for(int z=0; z<=7; z++){
                    if(!octree.find(x,y,z)){
                        pointFound = true;
                        octree.insert(x,y,z, refFilePath);
                        System.out.println(MessageFormat.format("Added index at point: x:{0} y:{1} z:{2}", x, y, z));
                        FileProcessor.saveObjectToFile(octree, octreeFilePath, false);

                        //Save index value to a file
                        valueIndex.put(indexName, new Point(x,y,z));
                        String indexFilePath = path+"Index"+strTableName+".ser";
                        FileProcessor.saveObjectToFile(valueIndex,indexFilePath,true);

                        break;
                    }
                }
                if(pointFound) break;
            }
            if(pointFound) break;
        }
    }

    private static void updateIndex(String indexName, String strTableName, String refFilePath, String operation) {

        String path = "src/main/resources/output/";
        String octreeFilePath = path+"Octree"+strTableName+".ser";

        if (valueIndex.containsKey(indexName)) {
            Point point = valueIndex.get(indexName);
            octree.remove(point.x, point.y, point.z);
            if (operation.equalsIgnoreCase("update")) {
                octree.insert(point.x, point.y, point.y, refFilePath);
                valueIndex.replace(indexName,point);
                System.out.println(MessageFormat.format("Updated index at point: x:{0} y:{1} z:{2}", point.x, point.y, point.z));
            }
            FileProcessor.saveObjectToFile(octree, octreeFilePath, false);
        }else{
            addValueToIndex(strTableName, indexName, refFilePath);
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
                        Vector<Table> list = FileProcessor.readFile(file.toString());
                        Vector<Table> updatedList = new Vector<>();
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
                                        String colName = key;
                                        String value = (String) htblColNameValue.get(key);
                                        updateIndex(colName+"#"+value,strTableName,file.toString(),"update");
                                    } catch (NoSuchFieldException | IllegalAccessException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                            }
                            updatedList.add(table);
                        }
                        FileProcessor.saveOrUpdateFile(file.toString(), updatedList, false);
                    }
                }

            }
        } catch (Exception e) {
            throw new DBAppException("Exception while updating row!! "+e.getMessage());
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
                        Vector<Table> rows = FileProcessor.readFile(file.toString());
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
                                        if (field.get(obj).toString().equalsIgnoreCase(value.toString())) {
                                            System.out.println("Deleting Row: "+obj);
                                            return true;
                                        }
                                    } catch (NoSuchFieldException | IllegalAccessException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return false;
                                });
                            });
                        System.out.println("Updated Row Count After Delete: "+rows.size());
                        if(rows.size()==0){
                            if (file.delete()) {
                                System.out.println("File deleted successfully: "+file.getName());
                            } else {
                                System.out.println("Failed to delete the file: "+file.getName());
                            }
                        }
                        FileProcessor.saveOrUpdateFile(file.toString(), rows, false);
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

        String sql = generateSql(arrSQLTerms, strarrOperators);
        System.out.println("Generated SQL: "+sql);

        Vector<Table> rows = readFullTable("Student");

        return rows.iterator();
    }

    private Vector<Table> readFullTable(String tableName) throws DBAppException {
        Vector<Table> rows = new Vector<>();

        String path = "src/main/resources/output/";
        File folder = new File(path);
        File[] files = folder.listFiles((dir, name) -> name.startsWith(tableName));
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    try {
                        rows.addAll(FileProcessor.readFile(file.toString()));
                    } catch (IOException | ClassNotFoundException e) {
                        throw new DBAppException("Exception while reading data from table!!");
                    }
                }
            }
        }
        return rows;
    }

    private Map<Vector<Table>, String> readFullTableWithFilePath(String tableName) throws DBAppException {
        Map<Vector<Table>, String> rowsWithFilePath = new HashMap<>();

        String path = "src/main/resources/output/";
        File folder = new File(path);
        File[] files = folder.listFiles((dir, name) -> name.startsWith(tableName));
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    try {
                        rowsWithFilePath.put(FileProcessor.readFile(file.toString()), file.toString());
                    } catch (IOException | ClassNotFoundException e) {
                        throw new DBAppException("Exception while reading data from table!!");
                    }
                }
            }
        }
        return rowsWithFilePath;
    }

    private String generateSql(SQLTerm[] arrSQLTerms,
                               String[] strarrOperators) throws DBAppException {
        String sqlStatement = "select * from ";
        String tableName = arrSQLTerms[0]._strTableName;
        List<String> validOperators = Arrays.asList(">", ">=", "<", "<=", "!=", "=");
        List<String> validStrArrOperators = Arrays.asList("AND","OR", "XOR");

        int index = 0;
        StringBuilder clause = new StringBuilder();
        for (SQLTerm arrSQLTerm : arrSQLTerms) {
            if(!validOperators.contains(arrSQLTerm._strOperator)) throw new DBAppException("Invalid Operator Passed: "+arrSQLTerm._strOperator);
            clause.append(arrSQLTerm._strColumnName)
                    .append(arrSQLTerm._strOperator)
                    .append(arrSQLTerm._objValue);

            if(index < strarrOperators.length){
                if(!validStrArrOperators.contains(strarrOperators[index])) throw new DBAppException("Invalid Operator Passed: "+strarrOperators[index]);
                clause.append(" ").append(strarrOperators[index]).append(" ");
            }
            index++;
        }

        sqlStatement = sqlStatement + tableName + " where " + clause;
        return sqlStatement;
    }

}
