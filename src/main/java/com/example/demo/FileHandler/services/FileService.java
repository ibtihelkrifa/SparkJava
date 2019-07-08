package com.example.demo.FileHandler.services;


import com.example.demo.SparkConnection.SparkConnection;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.springframework.stereotype.Service;
import scala.Serializable;

import javax.inject.Singleton;
import java.util.Arrays;
import java.util.Iterator;

@Service
public class FileService  implements Serializable{


    JavaSparkContext sc = SparkConnection.getContext();

    SparkSession ss = SparkConnection.getSession();

    public Integer getNumberWords()
    {



        //Resources.getResource("data/TestData.csv").getPath()
        JavaRDD<String> inputFIle= sparkContext.textFile("/home/ibtihel/Desktop/ProjectSparkJava/textSpark");



        JavaRDD<String> words = inputFIle.flatMap( new FlatMapFunction<String, String>() {

            @Override public   Iterator<String> call(String s) { return Arrays.asList(s.split(" ")).iterator(); }
        });

        return words.collect().size();

    }

}
