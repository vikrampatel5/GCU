package service;

import exceptions.DBAppException;
import model.Table;
import model.TableMetadata;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class FileProcessor {

    public static List<Table> readFile(String strTableName) throws IOException, ClassNotFoundException {


        File file = new File(strTableName);

        List<Table> rowList = new ArrayList<>();

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
}
