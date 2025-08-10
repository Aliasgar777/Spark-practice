package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class question17 {
//    17. Create a Java Spark job to pivot a dataset based on a category column.

    public void pivot(SparkSession spark){
        Dataset<Row> df = spark.read().option("header", "true")
                .option("inferSchema", "true").csv("src/main/resources/data/retail_sales.csv");


        Dataset<Row> pivotedDf = df.groupBy("Store", "Product")
                .pivot("Month")
                .sum("Sales");

        pivotedDf.show();

    }
}
