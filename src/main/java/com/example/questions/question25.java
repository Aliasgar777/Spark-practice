package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class question25 {

//    25. Write a Java Spark program to perform repartition and coalesce on a dataset.

    public void repartitionAndCoalesce(SparkSession spark){
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/people.csv");

        System.out.println("Initial Partition Count: " + df.rdd().getNumPartitions());

        Dataset<Row> repartitioned = df.repartition(4);
        System.out.println("After Repartition (4): " + repartitioned.rdd().getNumPartitions());
        System.out.println("---------------");


        Dataset<Row> coalesced = repartitioned.coalesce(2);
        System.out.println("After Coalesce (2): " + coalesced.rdd().getNumPartitions());
        System.out.println("---------------");

        // Show data
        coalesced.show();

    }
}
