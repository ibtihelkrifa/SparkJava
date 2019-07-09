package com.example.demo.FileHandler.services;


import com.example.demo.SparkConnection.SparkConnection;
import com.google.common.io.Resources;
import org.apache.commons.lang.StringUtils;
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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Service
public class TestService {

    transient JavaSparkContext sc = SparkConnection.getContext();

    transient SparkSession ss = SparkConnection.getSession();

    Logger logger = LoggerFactory.getLogger(FileService.class);

    public  String getEmptyCells(int idhash)
    {
        JavaRDD<String> inputFile= sc.textFile(Resources.getResource("Files/policy.csv").getPath());

        //JavaRDD<String> inputFile= sc.textFile(idhash+".csv");
        String header = inputFile.first();

        String[] columnNames = header.split(";",-1);

        JavaPairRDD<String,Integer> pairValue= inputFile.mapToPair(s->{
            String[] row_arr = s.split(";",-1);
            String idRow=row_arr[0];

            int numberOfElements = 0 ;
            for(String row_element : row_arr){
                if(StringUtils.isBlank(row_element)) numberOfElements++;
            }

                return new Tuple2<>(idRow,numberOfElements);
        });

            String s="{";
            JavaPairRDD<String,Integer> emptyPairs=pairValue.filter(t-> t._2>0);


            JavaRDD<String> list= emptyPairs.map(v->{
             String value="";
             value= v._1+":"+v._2;
             return value ;
            });


            List<String> listPairs=list.distinct().collect();

            String outputValue="";
            for(int i=0; i<listPairs.size();i++)
            {
                outputValue+=listPairs.get(i)+",";
            }

            outputValue="{"+outputValue+"}";



    return outputValue;


    }
}
