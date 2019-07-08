package com.example.demo.SparkConnection;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;



public class SparkConnection implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = -8907765312906929878L;
    //A name for the spark instance. Can be any string
    private static String appName = "Ibtihel-Spark-Spring";
    //Pointer / URL to the Spark instance - embedded
    private static String sparkMaster = "local[*]";

    private static JavaSparkContext spContext = null;
    private static SparkSession sparkSession = null;
    private static String tempDir = "file:///c:/temp/spark-warehouse";

    private static void getConnection() {

        if ( spContext == null) {
            //Setup Spark configuration
            SparkConf conf = new SparkConf()
                    .setAppName(appName)
//					.set("spark.sql.shuffle.partitions", "1")
//					.set("spark.default.parallelism", "6")
                    .set("spark.driver.memory", "10g")
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .setMaster(sparkMaster).set("spark.scheduler.mode", "FAIR");

            //Make sure you download the winutils binaries into this directory
            //from https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip
            System.setProperty("hadoop.home.dir", "c:\\winutils\\");

            //Create Spark Context from configuration
            spContext = new JavaSparkContext(conf);

            sparkSession = SparkSession
                    .builder()
                    .appName(appName)
                    .master(sparkMaster)
                    .config("spark.sql.warehouse.dir", tempDir)
                    .config("spark.scheduler.mode", "FAIR")
                    .getOrCreate();

        }

    }

    public static JavaSparkContext getContext() {

        if ( spContext == null ) {
            getConnection();
        }
        return spContext;
    }

    public static SparkSession getSession() {
        if ( sparkSession == null) {
            getConnection();
        }
        return sparkSession;
    }

}
