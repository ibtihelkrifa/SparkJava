package com.example.demo.FileHandler.services;

import com.example.demo.SparkConnection.SparkConnection;
import com.google.common.io.Resources;
import org.apache.avro.data.Json;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.jackson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.xml.sax.Parser;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;
import scala.util.parsing.json.JSONObject;
import scala.util.parsing.json.JSONObject$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


@Service
public class TestServiceSQL {

    transient JavaSparkContext sc = SparkConnection.getContext();

    transient SparkSession ss = SparkConnection.getSession();

    Logger logger = LoggerFactory.getLogger(FileService.class);


    public String getEmptyCells(int idhash) {

        Dataset<Row> dataset = ss.read().format("csv").option("header", "true").load(Resources.getResource("Files/"+idhash+".csv").getPath());

        String header[] = dataset.schema().fieldNames();


        String requete = "";

        System.out.println(header[0]);

        String[] colonnes = header[0].split(";");

        for (int i = 1; i < colonnes.length; i++) {
            requete = requete + "(CASE WHEN " + colonnes[i] + " IS NULL THEN 1 ELSE 0 END)+ ";
        }


        System.out.println(requete);

        dataset.createOrReplaceTempView("csvTab");


        Dataset<Row> dataset1 = ss.sql("select 'ID', SUM(  " + requete + " ) AS NbVide from csvTab group by ID ");
        dataset1.show();
        List<String> l = new ArrayList<>();

        dataset1.collectAsList().forEach(row -> {
            l.add(new String(row.getString(0) + ":" + row.getInt(1)));
        });

        String Value = "";

        for (int i = 0; i < l.size(); i++) {
            Value += l.get(i);
        }

        return Value;
    }
}
