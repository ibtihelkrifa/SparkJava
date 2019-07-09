package com.example.demo.FileHandler.services;


import com.example.demo.SparkConnection.SparkConnection;
import com.google.common.io.Resources;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class SnapshotService {

    transient JavaSparkContext sc = SparkConnection.getContext();

    transient SparkSession ss = SparkConnection.getSession();

    Logger logger = LoggerFactory.getLogger(FileService.class);

    public void unionPolicyFiles()
    {
        JavaRDD<String> inputFile1= sc.textFile(Resources.getResource("snapshot/policy1.csv").getPath());
        JavaRDD<String> inputFile2= sc.textFile(Resources.getResource("snapshot/policy2.csv").getPath());

        String[] allColumns1 = inputFile1.first().split(";",-1);
        String[] allColumns2=inputFile2.first().split(";",-1);

        List<String> listFields=getDistinctFields(allColumns1,allColumns2);




    }

    public List<String> getDistinctFields(String[] allColumns1, String[] allColumns2)
    {

    List<String> l1=Arrays.asList(allColumns1);
    List<String> l2=Arrays.asList(allColumns2);
    List<String> l3= new ArrayList();
        for (String l: l1)
    {
        l3.add(l);
    }

        for(String l:l2)
    {
        l3.add(l);
    }



    List<String> l4=l3.stream().distinct().collect(Collectors.toList());
    return  l4;
    }
}
