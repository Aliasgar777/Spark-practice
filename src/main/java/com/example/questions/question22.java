package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class question22 {

//    22. Implement a Java Spark job to tokenize sentences and count unique words.

    public void tokenize(SparkSession spark){

        Dataset<Row> sentences = spark.read()
                .textFile("src/main/resources/data/q1_input.txt").toDF("sentence");

        Dataset<Row> tokenized = sentences.withColumn(
                "words",
                split(lower(col("sentence")), "\\s+")
        );

        Dataset<Row> exploded = tokenized.select(explode(col("words")).alias("word"));

        Dataset<Row> cleaned = exploded.withColumn("word",
                regexp_replace(col("word"), "[^a-z]", ""));

        Dataset<Row> filtered = cleaned.filter(length(col("word")).gt(0));

        Dataset<Row> wordCount = filtered.groupBy("word").count().orderBy(desc("count"));

        wordCount.show(false);
    }
}
