package com.example.questions;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class question13 {

//    13. Implement a Java Spark job to calculate word co-occurrence in a text corpus.

    public void co_Occurences(SparkSession spark){

        Dataset<Row> words = spark.read()
                .textFile("src/main/resources/data/q1_input.txt")
                .toDF("value")
                .select(explode(split(lower(col("value")), "\\s+")).alias("word"))
                .filter(col("word").notEqual(""));

        Dataset<Row> wordPairs = words.crossJoin(words.withColumnRenamed("word", "context_word"))
                .filter(col("word").notEqual(col("context_word")));

        Dataset<Row> coOccurrenceCounts = wordPairs.groupBy("word", "context_word").count();

        coOccurrenceCounts.show();
    }

}

