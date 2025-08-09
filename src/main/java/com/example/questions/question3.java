package com.example.questions;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class question3 {

//    3. Write a Java Spark job to count occurrences of each character in a text file.

    public void countChar(SparkSession spark){
        Dataset<String> df = spark.read().textFile("src/main/resources/data/q2_input.txt");

        Dataset<String> characters = df.flatMap(
                (FlatMapFunction<String, String>) line -> Arrays.asList(line.split("")).iterator(),
                Encoders.STRING()
        ).filter((FilterFunction<String>) c -> !c.trim().isEmpty());

        Dataset<Row> charCounts = characters.groupBy("value").count();

        charCounts.show();
    }
}
