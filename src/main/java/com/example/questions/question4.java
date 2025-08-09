package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class question4 {
//    4. Load a CSV file using Java Spark and print the schema.

    public void printSchema(SparkSession spark){

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/people.csv");

        df.printSchema();
    }
}
