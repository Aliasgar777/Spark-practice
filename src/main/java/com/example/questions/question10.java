package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class question10 {

//    10. Load JSON data in Java Spark and extract specific fields.
    public void loadJson(SparkSession spark){
        Dataset<Row> people = spark.read()
                .option("multiline", "true")
                .option("inferSchema", "true")
                .json("src/main/resources/data/people.json");

        people.printSchema();
        people.show();
    }
}
