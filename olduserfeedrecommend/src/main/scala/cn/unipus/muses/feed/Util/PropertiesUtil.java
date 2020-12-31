package cn.unipus.muses.feed.Util;

import java.io.*;
import java.util.Properties;

public class PropertiesUtil {
    private static Properties prop = new Properties();

    private static String filePath = "D:\\myWork\\eclipse-workspace\\feed\\olduserfeedrecommend\\src\\main\\resources\\application.properties";

    static {
        try {
            File file = new File(filePath);
            if (file.exists()) {
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                prop.load(in);
            } else {
                String path = ClassLoader.getSystemClassLoader().getResource("application.properties").getPath();
                InputStream in = new BufferedInputStream(new FileInputStream(new File(path)));
                prop.load(in);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static Properties load(String filePath) throws IOException {
        Properties prop = new Properties();
        File file = new File(filePath);
        if (filePath.isEmpty() || !file.exists()) {
            String path = ClassLoader.getSystemClassLoader().getResource("application.properties").getPath();
            InputStream in = new BufferedInputStream(new FileInputStream(new File(path)));
            prop.load(in);
            return prop;
        }
        InputStream in = new BufferedInputStream(new FileInputStream(filePath));
        prop.load(in);
        return prop;
    }

    public static String getValue(String key) {
        return prop.getProperty(key);
    }

    public static void main(String[] args) {
        String prop = getValue("zookeeper.quorum");
        System.out.println(prop);
    }
}

