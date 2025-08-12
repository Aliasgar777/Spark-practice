package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class question24 {

//    24. Implement a Java Spark job to filter data based on multiple conditions.

    public void filter(SparkSession spark){
        Dataset<Row> peopleDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/people.csv");

        Dataset<Row> salaryDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/salary.csv");

        Dataset<Row> employeeDf = peopleDf.join(salaryDf,
                salaryDf.col("id").equalTo(peopleDf.col("id")), "inner");

        System.out.println("Original Data:");
        employeeDf.show();

        Dataset<Row> filtered = employeeDf.filter(col("age").gt(30).and(col("salary").gt(60000)));

        System.out.println("Filtered Data (Age > 30 and Salary > 60000):");
        filtered.show();

        Dataset<Row> filteredOr = employeeDf.filter(col("age").gt(30).or(col("city").equalTo("mumbai")));

        System.out.println("Filtered Data (Age > 30 or city = mumbai):");
        filteredOr.show();
    }
}
