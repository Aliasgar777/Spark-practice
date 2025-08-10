package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class question16 {

//    16. Implement a Java Spark program to filter out null or empty values from a dataset.

    public void filterNullEmptyVal(SparkSession spark){

        Dataset<Row> df = spark.read().option("header", "true")
                .option("inferSchema", "true").option("nullValue", "null").csv("src/main/resources/data/q16.csv");

        Dataset<Row> filtered = df.filter(
                col("date").isNotNull().and(length(trim(col("date"))).gt(0))
                        .and(col("product").isNotNull().and(length(trim(col("product"))).gt(0)))
                        .and(col("quantity").isNotNull().and(length(trim(col("quantity"))).gt(0)))
                        .and(col("price").isNotNull().and(length(trim(col("price"))).gt(0)))
        );

        filtered.show();

    }
}
