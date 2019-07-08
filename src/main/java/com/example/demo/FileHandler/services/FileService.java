package com.example.demo.FileHandler.services;


import com.example.demo.SparkConnection.SparkConnection;
import com.google.common.io.Resources;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Service
public class FileService  implements Serializable{



    transient JavaSparkContext sc = SparkConnection.getContext();

   transient SparkSession ss = SparkConnection.getSession();

    Logger logger = LoggerFactory.getLogger(FileService.class);

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

       JavaRDD<String> inputFile= sc.textFile(Resources.getResource("Files/policy.csv").getPath());
        String header = inputFile.first();
       String[] columnNames = header.split(";",-1);
        int columnsLength = columnNames.length;
        logger.info("total column size = " + columnsLength);



        JavaRDD<Integer> linesSize= inputFile.map(s -> {
            String[] row_arr = s.split(";",-1);
            int numberOfElements = 0 ;
            for(String row_element : row_arr){
                if(StringUtils.isNotBlank(row_element)) numberOfElements++;
            }
            return numberOfElements;

        });
        
        return linesSize.collect();

    }



    public void putEmptyColumns()
    {
        JavaRDD<String> inputFile= sc.textFile(Resources.getResource("Files/policy.csv").getPath());
        String newString="";

        JavaRDD<String> lines= inputFile.map(s->{
            String[] rows=s.split(";");
            String s1= new String();
            for(String rowElement: rows)
            {
                if(StringUtils.isBlank(rowElement))
                {
                    rowElement="Empty";


                }

                s1+=rowElement+";";



            }




            return  s1.substring(0, s1.length()-1);
        });



        lines.saveAsTextFile("Files/files.csv");

    }





 /*   public List<Integer> getNumberColumnsFlat()
    {
        JavaRDD<String> inputFIle= sc.textFile(Resources.getResource("Files/policy.csv").getPath());
    }*/

}
