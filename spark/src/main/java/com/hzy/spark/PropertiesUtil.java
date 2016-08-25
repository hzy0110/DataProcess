package com.hzy.spark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Hzy on 2016/6/14.
 */
public class PropertiesUtil {
    private static Properties properties = null;
    private static final Logger logger = LogManager.getLogger(PropertiesUtil.class);
    public static void main(String[] args) throws IOException {
        //logger.info(getValue("channlelevel"));
        //logger.info("223322");
    }

    public static String getValue(String name){
        try {
            Properties properties = new Properties();
            //InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream("/config.properties");
            InputStream input = new BufferedInputStream(new FileInputStream(System.getProperty("user.dir")+ "/spark/config.properties"));
            properties.load(input);// 加载属性文件
            input.close();
            return properties.getProperty(name);
        }
        catch (IOException e){
            return null;
        }

    }
}
