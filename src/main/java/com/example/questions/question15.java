package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class question15 {

//    15. Use Java Spark to convert a CSV file to Parquet format.

    public void CsvToParquet(SparkSession spark){

        Dataset<Row> csvDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/people.csv");

        csvDf.show();

        csvDf.write()
                .mode("overwrite")
                .parquet("src/main/resources/data/people_parquet");

    }
}
