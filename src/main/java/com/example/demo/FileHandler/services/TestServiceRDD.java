package com.example.demo.FileHandler.services;


import com.example.demo.FileHandler.userDefinedException.UnionNumberFIleException;
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


    public void dynamicUnionRDD(List<String> HashFilesList) throws UnionNumberFIleException {

        if (HashFilesList.size() > 1) {
            List<String> allheaders = new ArrayList<>();
            List<List<String>> ListFileContainsListSingleHeaders = new ArrayList<>();
            List<String> ListDataset = new ArrayList<>();

            for (String file : HashFilesList) {
                JavaRDD<String> inputFile1 = sc.textFile(Resources.getResource("Files/" + file + ".csv").getPath());
                Dataset<Row> d1 = getDataFrame(inputFile1);
                String header1 = inputFile1.first();
                List<String> headerFile1 = new ArrayList<>(Arrays.asList(header1.split(";", -1)));
                allheaders.addAll(headerFile1);
                ListFileContainsListSingleHeaders.add(headerFile1);
                d1.createOrReplaceTempView(file);
                ListDataset.add(file);

            }


            allheaders = allheaders.stream().distinct().collect(Collectors.toList());

            List<String> ListQuery = new ArrayList<>();

            for (List<String> ListSingleFileHeaders : ListFileContainsListSingleHeaders) {
                String query = createQuerry(allheaders, ListSingleFileHeaders);
                ListQuery.add(query);
            }

            String finalQuery = "select " + ListQuery.get(0) + " from " + ListDataset.get(0) + " union ";

            for (int i = 1; i < ListQuery.size(); i++) {
                finalQuery += "( select " + ListQuery.get(i) + " from " + ListDataset.get(i) + " ) union  ";
            }

            finalQuery = finalQuery.substring(0, finalQuery.length() - 7);
            Dataset<Row> d = ss.sql(finalQuery);
            System.out.println(finalQuery);
            d.show();

        } else {
            throw new UnionNumberFIleException("l'union se fait entre au minimum deux fichiers");
        }

    }


    public void UnionWithRDD(int idhash1, int idhash2) {
        JavaRDD<String> inputFile1 = sc.textFile(Resources.getResource("Files/test1.csv").getPath());
        JavaRDD<String> inputFile2 = sc.textFile(Resources.getResource("Files/test2.csv").getPath());

        Dataset<Row> d1 = getDataFrame(inputFile1);
        Dataset<Row> d2 = getDataFrame(inputFile2);


        String header1 = inputFile1.first();

        List<String> headerFile1 = new ArrayList<>(Arrays.asList(header1.split(";", -1)));

        String header2 = inputFile2.first();

        List<String> headerFile2 = new ArrayList<>(Arrays.asList(header2.split(";", -1)));


        List<String> allHeaders = new ArrayList<>();
        allHeaders.addAll(headerFile1);
        allHeaders.addAll(headerFile2);
        allHeaders = allHeaders.stream().distinct().collect(Collectors.toList());


        d1.createOrReplaceTempView("d1");
        d2.createOrReplaceTempView("d2");
        Dataset<Row> d = ss.sql("select " + createQuerry(allHeaders, headerFile1) + " from d1 union ( select " + createQuerry(allHeaders, headerFile2) + " from d2 ) ");

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


    /**
     * this function takes allheaders / and Single File header and constructs a querry for union
     */

    public String createQuerry(List<String> allHeaders, List<String> SingleFileHeader) {
        String querry = "";


        for (int i = 0; i < allHeaders.size(); i++) {
            if (!SingleFileHeader.contains(allHeaders.get(i))) {
                allHeaders.set(i, null);
            }
        }

        for (String e : allHeaders) {
            querry += "," + e;
        }
        System.out.println(querry);
        return querry.substring(1);

    }



    public void UnionRDD()
    {
        List<String> allHeaders= new ArrayList<>();
        List<List<String>> listFilesContainsListHeaders= new ArrayList<>();

        List<List<String>> files= new ArrayList<>();

        List<String> idhashFiles= new ArrayList<>();
        idhashFiles.add("test1");
        idhashFiles.add("test2");

        for(int i=0;i< idhashFiles.size();i++)
        {
            JavaRDD<String> inputFile = sc.textFile(Resources.getResource("Files/"+idhashFiles.get(i)+".csv").getPath());
            List<String> fileHeader= Arrays.asList(inputFile.first().split(";"));
            allHeaders.addAll(fileHeader);
            listFilesContainsListHeaders.add(fileHeader);
            files.add(inputFile.collect());



        }

        allHeaders.stream().distinct();
        JavaRDD<String> finalSingleFileListHeaders= sc.parallelize(listFilesContainsListHeaders).map(fileListHeader -> {


            for(int i=0; i< allHeaders.size();i++)
            {
                if(! fileListHeader.contains(allHeaders.get(i)))
                {
                    allHeaders.set(i,null);
                }
            }

            String stringHeaders="";
            for(String e: allHeaders)
            {
                stringHeaders+= ";"+e;
            }

            return stringHeaders.substring(1);

        });


        for(int i=0; i< files.size();i++)
        {
            System.out.println(finalSingleFileListHeaders.collect().get(i));
          //  files.get(i).set(0,finalSingleFileListHeaders.collect().get(i));
        }



       JavaRDD<JavaRDD<String>>  RDDFiles=sc.parallelize(files).map(file-> sc.parallelize(file));

      List<JavaRDD<String>> listRddFiles=  RDDFiles.collect();

       JavaRDD<String> finalFile= listRddFiles.get(0);

       for(int i=1; i< listRddFiles.size();i++)
       {
           finalFile.union(listRddFiles.get(i));
       }

       JavaRDD<Row> finalRddRowFile=finalFile.map(s-> RowFactory.create(s));

       List<StructField> listStructFields= new ArrayList<>();
      JavaRDD<StructField> listRDDStructFields= sc.parallelize(allHeaders).map(header -> new StructField(header, DataTypes.StringType,true, Metadata.empty()));
       listStructFields= listRDDStructFields.collect();

       StructType schema= new StructType(listStructFields.toArray(new StructField[0]));

       Dataset<Row> dataset= ss.createDataFrame(finalRddRowFile, schema);

       dataset.show();


    }





}
