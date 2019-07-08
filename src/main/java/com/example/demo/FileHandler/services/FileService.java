package com.example.demo.FileHandler.services;


import com.example.demo.SparkConnection.SparkConnection;
import com.google.common.io.Resources;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;
import scala.Serializable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Service
public class FileService  implements Serializable{



    transient JavaSparkContext sc = SparkConnection.getContext();

   transient SparkSession ss = SparkConnection.getSession();



    public Integer getNumberWords()
    {


        JavaRDD<String> inputFIle= sc.textFile(Resources.getResource("Files/textSpark").getPath());



        JavaRDD<String> words = inputFIle.flatMap( new FlatMapFunction<String, String>() {

            @Override public   Iterator<String> call(String s) { return Arrays.asList(s.split(" ")).iterator(); }
        });

        return words.collect().size();

    }

    public List<Integer> getNumberColumns()
    {
        JavaRDD<String> inputFIle= sc.textFile(Resources.getResource("Files/policy.csv").getPath());
        JavaRDD<Integer> linesSize= inputFIle.map(s -> s.split(";").length );

        return linesSize.collect();

    }


 /*   public List<Integer> getNumberColumnsFlat()
    {
        JavaRDD<String> inputFIle= sc.textFile(Resources.getResource("Files/policy.csv").getPath());
    }*/

}