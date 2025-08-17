package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.broadcast;

public class question27 {

//    27. Implement a Java Spark program to use broadcast variables for a small lookup table.
    public void broadcastVariables(SparkSession spark){
        Dataset<Row> peopleDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/q27_people.csv");

        Dataset<Row> countryDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/country.csv");


        Dataset<Row> result = peopleDF
                .join(broadcast(countryDF),
                        peopleDF.col("countryCode").equalTo(countryDF.col("code")))
                .select("name", "country");

        result.show();
    }
}
