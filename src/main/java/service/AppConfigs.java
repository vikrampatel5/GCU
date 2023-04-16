package service;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class AppConfigs {
    private static AppConfigs appConfigs = null;
    Properties configs = new Properties();

    private AppConfigs() {
        // Private constructor to prevent instantiation outside of this class
    }

    public static AppConfigs getInstance() {
        if (appConfigs == null) {
            appConfigs = new AppConfigs();
        }
        return appConfigs;
    }

    public Properties getConfigs() {
        return this.configs;
    }

    public void loadConfigs(String configPath){
        try {
            FileInputStream fis = new FileInputStream(configPath);
            configs.load(fis);
            System.out.println(configs.getProperty("MaximumRowsCountinTablePage"));
            System.out.println(configs.getProperty("MaximumEntriesinOctreeNode"));
        } catch (IOException e) {
            System.err.println("Error reading properties file: " + e.getMessage());
        }
    }
}
