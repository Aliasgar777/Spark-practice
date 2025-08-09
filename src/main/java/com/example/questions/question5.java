package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class question5 {

//    5. Implement a Java Spark application to compute average, min, max of numeric column in CSV.

    public void avgMinMax(SparkSession spark) {

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/people.csv");

        df.createOrReplaceTempView("people");

        Dataset<Row> result = df.agg(
                avg("age").alias("Average Age"),
                min("age").alias("Min Age"),
                max("age").alias("Max Age")
        );

        result.show();

    }

}
