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
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import javax.xml.validation.Schema;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class TestServiceRDD {

    transient JavaSparkContext sc = SparkConnection.getContext();

    transient SparkSession ss = SparkConnection.getSession();

    Logger logger = LoggerFactory.getLogger(FileService.class);

    public  String getEmptyCells(int idhash)
    {

        JavaRDD<String> inputFile= sc.textFile(idhash+".csv");
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


    public void UnionWithRDD2(int idhash1, int idhash2)
    {

        JavaRDD<String> inputFile1= sc.textFile(Resources.getResource("Files/test1.csv").getPath());
        JavaRDD<String> inputFile2= sc.textFile(Resources.getResource("Files/test2.csv").getPath());

        String header1= inputFile1.first();

        String[] headerFile1= header1.split(";",-1);

        String header2= inputFile2.first();

        String[] headerFile2= header2.split(";",-1);
       /* List<String> mergedList= new ArrayList<>();



      *//*  for(int i=0; i< headerFile1.length; i++)
        {
           mergedList.add(headerFile1[i]);
           l1+=","+headerFile1[i];

        }*//*
  //new


        List<String> final1= new ArrayList<>();

       */


        String l1="";
        String l2="";

        List<StructField> structFields1= new ArrayList<>();
        for(String col1: headerFile1)
        {
            l1+=", "+ col1;
            structFields1.add(new StructField(col1, DataTypes.StringType,true, Metadata.empty()));
        }

        List<StructField> structFields2= new ArrayList<>();
        for(String col2: headerFile2)
        {
            l2+=", "+col2;
            structFields2.add(new StructField(col2, DataTypes.StringType,true, Metadata.empty()));
        }

      /*  List<StructField> structFields3= new ArrayList<>();

        for(String col3: mergedList)
        {
            structFields3.add(new StructField(col3,DataTypes.StringType,true,Metadata.empty()));
        }*/

        StructType schema1= new StructType(structFields1.toArray(new StructField[0]));
        StructType schema2= new StructType(structFields2.toArray(new StructField[0]));
      //  StructType mergedSchema= new StructType(structFields3.toArray(new StructField[0]));

        JavaRDD<Row> listRow1= inputFile1.map(s-> RowFactory.create(s));
        JavaRDD<Row> listRow2= inputFile2.map(s-> RowFactory.create(s));


        Dataset<Row> datset1= ss.createDataFrame(listRow1,schema1);
        Dataset<Row> datset2= ss.createDataFrame(listRow2,schema2);

        datset1.createOrReplaceTempView("d1");
        datset2.createOrReplaceTempView("d2");

        l1=l1.substring(1);
        l2=l2.substring(1);

        System.out.println(l1);
        Dataset<Row> d=ss.sql("select "+ l2 +" from d2 ");

        d.coalesce(1)
                .write()
                .mode ("overwrite")
                .mode("append")
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save("outputFiles/"+55);








    }

    public void UnionWithRDD(int idhash1, int idhash2)
    {
        Dataset<Row> d1=getDataFrame(idhash1+".csv");
        Dataset<Row> d2=getDataFrame(idhash2+".csv");

            Dataset<Row> d3=d1.union(d2);

            d3.show();


    }


    public Dataset<Row> getDataFrame(String path)
    {
        JavaRDD<String> inputFile1= sc.textFile(Resources.getResource("Files/"+path).getPath());

        String[] allColumns1 = inputFile1.first().split(";",-1);

        JavaRDD<String[]> lines1= inputFile1.map(s->{
            String[] rows=s.split(";",-1);


            return  rows;
        });


        List<StructField> structFieldList1 =new ArrayList<>();
        for( String col : allColumns1) {
            structFieldList1.add(new StructField(col, DataTypes.StringType,true, Metadata.empty()));
        }


        StructType schema = new StructType(structFieldList1.toArray(new StructField[0]));

        JavaRDD<Row> linesRow1 = lines1.map(s-> RowFactory.create(s));


        Dataset<Row> dataFrame1 = ss.createDataFrame(linesRow1,schema);
        return dataFrame1;
    }

}
