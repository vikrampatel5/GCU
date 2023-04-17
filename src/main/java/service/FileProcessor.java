package service;

import exceptions.DBAppException;
import model.Table;
import model.TableMetadata;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Vector;

public class FileProcessor {

    public static List<Table> readFile(String strTableName) throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(strTableName);
        ObjectInputStream ois = new ObjectInputStream(fis);
        List<Table> objects = (List<Table>) ois.readObject();
        ois.close();
        return objects;
    }

    public static void saveOrUpdateFile(String filePath, List<Table> list) throws IOException {

        // Create an instance of FileOutputStream and ObjectOutputStream classes
        FileOutputStream fos = new FileOutputStream( filePath, true);
        ObjectOutputStream oos = new ObjectOutputStream(fos);

        // Write modified objects to the file
        oos.writeObject(list);
        oos.close();
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
}
