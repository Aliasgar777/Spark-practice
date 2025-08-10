package com.example.questions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;

public class question12 {

//    12. Create a Java Spark program to find the top 5 most frequent words in a text file.

    public void top5Words(SparkSession spark){

        Dataset<String> df = spark.read().textFile("src/main/resources/data/q2_input.txt");

        Dataset<String> words = df.flatMap(
                (FlatMapFunction<String, String>) line -> Arrays.asList(line.split("\\s+")).iterator(),
                Encoders.STRING()
        );

        Dataset<Row> wordCount = words.groupBy("value").count().orderBy(col("count").desc()).limit(5);

        wordCount.show();
    }
}
