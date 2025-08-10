package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class question8 {

//    8. Create a Java Spark program to remove duplicates from a dataset.

    public void removeDuplicates(SparkSession spark){

        Dataset<Row> people_duplicate = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/people_duplicates.csv");

        Dataset<Row> distinct = people_duplicate.distinct();

        distinct.show();
    }

}
