package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.desc;

public class question9 {

//    9. Write a Java Spark application to sort data by a numeric column in descending order.

    public void sortByColumn(SparkSession spark){
        Dataset<Row> people_duplicate = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/people.csv");

        String colName = "age";

        Dataset<Row> sorted = people_duplicate.orderBy(desc(colName));

        sorted.show();
    }
}
