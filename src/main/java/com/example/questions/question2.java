package com.example.questions;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.Array;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lower;

public class question2 {
    // 2. Implement a Java Spark program to filter lines containing a specific keyword.

    public void filterLinesByKeyword(SparkSession spark){

        Dataset<String> df = spark.read().textFile("src/main/resources/data/q2_input.txt");

        String keyword = "data";

        Dataset<String> filtered = df.filter(
                (FilterFunction<String>) line ->
                line != null && line.toLowerCase().contains(keyword.toLowerCase()));

        filtered.show(false);

    }

}
