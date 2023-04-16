package service;

import model.Table;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.*;
import java.util.Hashtable;
import java.util.List;

public class FileProcessor {

    public static List<Table> readFile(String strTableName) throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(strTableName + ".ser");
        ObjectInputStream ois = new ObjectInputStream(fis);
        List<Table> objects = (List<Table>) ois.readObject();
        ois.close();
        return objects;
    }

    public static void saveOrUpdateFile(String strTableName, List<Table> list) throws IOException {
        // Create an instance of FileOutputStream and ObjectOutputStream classes
        FileOutputStream fos = new FileOutputStream(strTableName + ".ser", true);
        ObjectOutputStream oos = new ObjectOutputStream(fos);

        // Write modified objects to the file
        oos.writeObject(list);
        oos.close();
        fos.close();
    }

    public static void saveMetaData(String fileNameWithPath, Hashtable hashtable) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileNameWithPath));
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
            csvPrinter.printRecord(hashtable.keySet());
            csvPrinter.printRecord(hashtable.values());
            csvPrinter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
