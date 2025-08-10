package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.sum;

public class question14 {

//    14. Write a Java Spark application to compute running total of sales over time.

    public void runningTotal(SparkSession spark) {

        Dataset<Row> salesDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/q14_sale.csv");

        WindowSpec windowSpec = Window.orderBy("date")
                .rowsBetween(Window.unboundedPreceding(), Window.currentRow());

        Dataset<Row> runningTotalDF = salesDF
                .withColumn("running_total", sum("sales").over(windowSpec));

        runningTotalDF.show();
    }
}