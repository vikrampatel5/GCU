package service;

import exceptions.DBAppException;
import model.Table;
import model.TableMetadata;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Vector;

public class FileProcessor {

    public static Vector<Table> readFile(String strTableName) throws IOException, ClassNotFoundException {


        File file = new File(strTableName);

        Vector<Table> rowList = new Vector<>();

        if (file.length() != 0) {

            //Create new FileInputStream object to read file
            FileInputStream fis = new FileInputStream(strTableName);
            //Create new ObjectInputStream object to read object from file
            ObjectInputStream ois = new ObjectInputStream(fis);

            while (fis.available() != 0) {
                rowList.add((Table) ois.readObject());
            }
            ois.close();
            fis.close();
        }


        return rowList;
    }

    public static void saveOrUpdateFile(String filePath, List<Table> list, boolean append) throws IOException {

        File f = new File(filePath);

        // Create an instance of FileOutputStream and ObjectOutputStream classes
        FileOutputStream fos = new FileOutputStream(filePath, append);

        if (f.length() == 0) {
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            for (Table row : list)
                oos.writeObject(row);
            oos.close();
        }
        // There is content in file to be write on
        else {

            CustomOutputStream oos = new CustomOutputStream(fos);
            for (Table row : list)
                oos.writeObject(row);

            // Closing the FileOutputStream object
            // to release memory resources
            oos.close();
        }
        fos.close();
    }

    public static void saveMetaData(String fileNameWithPath, Vector<TableMetadata> metadata) throws DBAppException {

        try {
            Writer writer = Files.newBufferedWriter(Paths.get(fileNameWithPath));
            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("TableName", "ColumnName", "ColumnType", "ClusteringKey", "IndexName", "IndexType", "Min", "Max"));

            metadata.forEach(row -> {
                try {
                    csvPrinter.printRecord(row.getTableName(),
                            row.getColumnName(),
                            row.getColumnType(),
                            row.getClusteringKey(),
                            row.getIndexName(),
                            row.getIndexType(),
                            row.getMin(),
                            row.getMax()
                    );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            csvPrinter.flush();
            csvPrinter.close();
            writer.close();
        } catch (Exception e) {
            throw new DBAppException("Exception while writing metadata file.");
        }

    }

    public static Vector<TableMetadata> readMetadata(String filePath) throws IOException {

        Vector<TableMetadata> metadataList = new Vector<>();
        try (
                Reader reader = Files.newBufferedReader(Paths.get(filePath));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
        ) {

            TableMetadata metadata;
            for (CSVRecord csvRecord : csvParser) {
                metadata = new TableMetadata();
                metadata.setTableName(csvRecord.get(0));
                metadata.setColumnName(csvRecord.get(1));
                metadata.setColumnType(csvRecord.get(2));
                metadata.setClusteringKey(Boolean.parseBoolean(csvRecord.get(3)));
                metadata.setIndexName(csvRecord.get(4));
                metadata.setIndexType(csvRecord.get(5));
                metadata.setMin(csvRecord.get(6));
                metadata.setMin(csvRecord.get(7));

                metadataList.add(metadata);
            }

            return metadataList;
        }
    }

    public static void saveObjectToFile(Object obj, String filePath, boolean append) {
        try {
            File f = new File(filePath);

            // Create an instance of FileOutputStream and ObjectOutputStream classes
            FileOutputStream fos = new FileOutputStream(filePath, append);

            if (f.length() == 0) {
                ObjectOutputStream oos = new ObjectOutputStream(fos);
                oos.writeObject(obj);
            }
            else {

                CustomOutputStream oos = new CustomOutputStream(fos);
                oos.writeObject(obj);
                oos.close();
            }
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Octree<String> loadOctreeFromFile() {
        String path = "src/main/resources/output/";
        String tableName = "IndexStudent.ser";

        String filePath = path+tableName;
        Octree<String> octree = null;
        try {
            File file = new File(filePath);
            if (file.exists()) {
                System.out.println("Loading Octree indexes!! from file: "+filePath);
                FileInputStream fileIn = new FileInputStream(filePath);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                octree = (Octree<String>) in.readObject();
                in.close();
                fileIn.close();
            } else {
                System.out.println(filePath + " : File does not exist.");
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return octree;
    }

    public static void deleteExistingData(String filePath) {

        File directory = new File(filePath);
        for (File file : directory.listFiles()) {
            if (!file.isDirectory()) {
                boolean result = file.delete();
                if (result) System.out.println("Deleted: " + file.getName());
            }
        }
    }
}
