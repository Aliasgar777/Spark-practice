package com.example.questions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class question1 {

    // 1. Create a Spark application in Java to count words in a text file.

    public void countWords(SparkSession spark){

        Dataset<String> textFile = spark.read()
                .textFile("src/main/resources/data/q1_input.txt");

        Dataset<String> words = textFile.flatMap(
                (FlatMapFunction<String, String>) line -> Arrays.asList(line.split("\\s+")).iterator(),
                Encoders.STRING()
        );

        words.show();

        Dataset<Row> wordCounts = words.groupBy("value").count();

        wordCounts.show();


    }
}
