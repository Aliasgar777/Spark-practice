package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;


public class question20 {

//    20. Create a Java Spark job to calculate moving average of a numeric column.

    public void movingfAvg(SparkSession spark){
        Dataset<Row> employeeDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/employees.csv");

        WindowSpec windowSpec = Window.orderBy("EmpId")
                        .rowsBetween(-2, 0); // Last 2 rows + current row

        Dataset<Row> result = employeeDf.withColumn(
                "Moving_avg_salary",
                avg(col("Salary")).over(windowSpec)
        );

        result.select("EmpId", "Name", "Salary", "Moving_avg_salary").show();

    }

}
