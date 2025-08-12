package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class question23 {

//    23. Create a Java Spark program to calculate correlation between two numeric columns.

    public void correlation(SparkSession spark){
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

        double corr = employeeDf.stat().corr("age", "salary");

        employeeDf.show();

        System.out.println("Correlation between Age and Salary: " + corr);

    }
}
