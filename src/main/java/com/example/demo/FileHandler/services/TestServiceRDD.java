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
import java.util.stream.Collectors;

@Service
public class TestServiceRDD {

    transient JavaSparkContext sc = SparkConnection.getContext();

    transient SparkSession ss = SparkConnection.getSession();

    Logger logger = LoggerFactory.getLogger(FileService.class);


    public String getEmptyCells(int idhash) {

        JavaRDD<String> inputFile = sc.textFile(idhash + ".csv");
        String header = inputFile.first();

        String[] columnNames = header.split(";", -1);

        JavaPairRDD<String, Integer> pairValue = inputFile.mapToPair(s -> {
            String[] row_arr = s.split(";", -1);
            String idRow = row_arr[0];

            int numberOfElements = 0;
            for (String row_element : row_arr) {
                if (StringUtils.isBlank(row_element)) numberOfElements++;
            }

            return new Tuple2<>(idRow, numberOfElements);
        });

        String s = "{";
        JavaPairRDD<String, Integer> emptyPairs = pairValue.filter(t -> t._2 > 0);


        JavaRDD<String> list = emptyPairs.map(v -> {
            String value = "";
            value = v._1 + ":" + v._2;
            return value;
        });


        List<String> listPairs = list.distinct().collect();

        String outputValue = "";
        for (int i = 0; i < listPairs.size(); i++) {
            outputValue += listPairs.get(i) + ",";
        }

        outputValue = "{" + outputValue + "}";


        return outputValue;


    }


    public void UnionWithRDD(int idhash1, int idhash2) {
        JavaRDD<String> inputFile1 = sc.textFile(Resources.getResource("Files/" + idhash1 + ".csv").getPath());
        JavaRDD<String> inputFile2 = sc.textFile(Resources.getResource("Files/" + idhash2 + ".csv").getPath());

        Dataset<Row> d1 = getDataFrame(inputFile1);
        Dataset<Row> d2 = getDataFrame(inputFile2);


        String header1 = inputFile1.first();

        List<String> headerFile1 = new ArrayList<>(Arrays.asList(header1.split(";", -1)));

        String header2 = inputFile2.first();

        List<String> headerFile2 = new ArrayList<>(Arrays.asList(header2.split(";", -1)));


        List<String> allHeaders = new ArrayList<>();
        allHeaders.addAll(headerFile1);
        allHeaders.addAll(headerFile2);



        d1.createOrReplaceTempView("d1");
        d2.createOrReplaceTempView("d2");
        Dataset<Row> d = ss.sql("select " + createQuerry(allHeaders,headerFile1) + " from d1 union ( select " + createQuerry(allHeaders,headerFile2) + " from d2 ) ");

        d.show();

    }


    public Dataset<Row> getDataFrame(JavaRDD<String> inputFile1) {

        String[] allColumns1 = inputFile1.first().split(";", -1);

        List<String[]> lignes1 = inputFile1.map(s -> {

            String[] rows = s.split(";", -1);


            return rows;
        }).collect();

        List<String[]> sublignes1 = lignes1.subList(1, lignes1.size());

        JavaRDD<String[]> lines1 = sc.parallelize(sublignes1);


        List<StructField> structFieldList1 = new ArrayList<>();
        for (String col : allColumns1) {
            structFieldList1.add(new StructField(col, DataTypes.StringType, true, Metadata.empty()));
        }


        StructType schema = new StructType(structFieldList1.toArray(new StructField[0]));


        JavaRDD<Row> linesRow1 = lines1.map(s -> RowFactory.create(s));


        Dataset<Row> dataFrame1 = ss.createDataFrame(linesRow1, schema);
        return dataFrame1;
    }





    /** this function takes allheaders / and Single File header and constructs a querry for union*/

    public String createQuerry(List<String> allHeaders , List<String> SingleFileHeader){
        String querry = "";


        return querry;
    }

}
