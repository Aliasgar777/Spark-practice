package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class question6 {

//    6. Write a Java Spark program to join two datasets based on a common key.

    public void joinDataset(SparkSession spark){
        Dataset<Row> people_df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/people.csv");

        Dataset<Row> salary_df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/salary.csv");

        Dataset<Row> joined = people_df.join(salary_df, "id");

        joined.show();

    }

}
