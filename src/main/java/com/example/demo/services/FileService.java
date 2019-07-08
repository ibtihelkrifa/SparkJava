package com.example.demo.services;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.springframework.stereotype.Service;
import scala.Serializable;
import scala.Tuple2;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

@Service
public class FileService  implements Serializable{

    @Singleton
     SparkConf sparkConf= new SparkConf().setAppName("TestApplication").setMaster("local[*]");
    @Singleton
       transient JavaSparkContext sparkContext= new JavaSparkContext(sparkConf);

    public Integer getNumberWords()
    {
        JavaRDD<String> inputFIle= this.sparkContext.textFile("/home/ibtihel/Desktop/ProjectSparkJava/textSpark");


        JavaRDD<String> words = inputFIle.flatMap( new FlatMapFunction<String, String>() {

            @Override public   Iterator<String> call(String s) { return Arrays.asList(s.split(" ")).iterator(); }
        });

        return words.collect().size();

    }

}
