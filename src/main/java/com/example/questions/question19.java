package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class question19 {

//    19. Write a Java Spark program to perform inner, left, and right joins on datasets.

    public void joins(SparkSession spark){
        Dataset<Row> employeeDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/employees.csv");

        Dataset<Row> departmentDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/departments.csv");

        employeeDf.show();
        departmentDf.show();

        Dataset<Row> innerJoin =  employeeDf.join(departmentDf,
                employeeDf.col("DeptID").equalTo(departmentDf.col("DeptID"))
                ,"inner");
        innerJoin.show();

        Dataset<Row> leftJoin = employeeDf.join(departmentDf,
                employeeDf.col("DeptID").equalTo(departmentDf.col("DeptID")),
                "left");
        leftJoin.show();

        Dataset<Row> rightJoin = employeeDf.join(departmentDf,
                employeeDf.col("DeptID").equalTo(departmentDf.col("DeptID")),
                "right");
        rightJoin.show();

    }
}
