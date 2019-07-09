package com.example.demo.FileHandler.services;


import com.example.demo.SparkConnection.SparkConnection;
import com.google.common.io.Resources;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
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



  /*  public void putEmptyColumns()
    {
        JavaRDD<String> inputFile= sc.textFile(Resources.getResource("Files/policy.csv").getPath());

        String[] allColumns = inputFile.first().split(";",-1);

        JavaRDD<String[]> lines= inputFile.map(s->{
            String[] rows=s.split(";",-1);

            for(int i = 0 ; i < rows.length ; i++)
            {
                if(StringUtils.isBlank(rows[i]))
                {
                    rows[i]="Empty";
                }
            }




            return  rows;
        });


        List<StructField> structFieldList =new ArrayList<>();
        for( String col : allColumns) {
            structFieldList.add(DataTypes.createStructField(col, DataTypes.StringType,true,Metadata.empty()));
        }


        StructType schema = DataTypes.createStructType(structFieldList);
        JavaRDD<Row> linesRow = lines.map(s-> RowFactory.create(s));


        Dataset<Row> dataFrameWithEmpty = ss.createDataFrame(linesRow,schema);

        dataFrameWithEmpty.show();
    }*/




    public void putEmptyColumns2()
    {
        JavaRDD<String> inputFile= sc.textFile(Resources.getResource("Files/policy.csv").getPath());

        String[] allColumns = inputFile.first().split(";",-1);

        JavaRDD<String[]> lines= inputFile.map(s->{
            String[] rows=s.split(";",-1);

            for(int i = 0 ; i < rows.length ; i++)
            {
                if(StringUtils.isBlank(rows[i]))
                {
                    rows[i]="Empty";
                }
            }




            return  rows;
        });


        List<StructField> structFieldList =new ArrayList<>();
        for( String col : allColumns) {
            structFieldList.add(new StructField(col, DataTypes.StringType,true, Metadata.empty()));
        }


        StructType schema = new StructType(structFieldList.toArray(new StructField[0]));

        JavaRDD<Row> linesRow = lines.map(s-> RowFactory.create(s));


        Dataset<Row> dataFrameWithEmpty = ss.createDataFrame(linesRow,schema);

        dataFrameWithEmpty.show();

        dataFrameWithEmpty.coalesce(1)
                .write()
                .mode ("overwrite")
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save("filename.csv");



    }




    public void flatMapAndReduceByKeyExample()
    {
        JavaRDD<String> inputFile= sc.textFile(Resources.getResource("Files/textSpark").getPath());



        JavaRDD<String> words = inputFile.flatMap(s->
              Arrays.asList(s.split(" ")).iterator()

        );


        JavaPairRDD<String,Integer> pairWords= words.mapToPair(word-> new Tuple2(word,1));
        JavaPairRDD<String,Integer> mapValues=pairWords.mapValues(v-> v+1);

        JavaPairRDD<String,Integer> counts = mapValues.reduceByKey((a,b)-> a + b );

        JavaPairRDD<String,java.lang.Iterable<Integer>> wordsGroupedByKey= pairWords.groupByKey();

        counts.saveAsTextFile("counts");
        wordsGroupedByKey.saveAsTextFile("groupByKey");

    }








}