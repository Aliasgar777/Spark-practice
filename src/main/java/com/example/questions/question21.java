package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

public class question21 {

//    21. Use Java Spark to perform a simple ETL pipeline: read CSV, transform, and write to Parquet.

    public void etlPipeline(SparkSession spark){
        Dataset<Row> employeesDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true") // Automatically detect column types
                .csv("src/main/resources/data/employees.csv");

        System.out.println("--- Original Data ---");
        employeesDF.show();

        Dataset<Row> transformedDF = employeesDF
                .filter(col("Salary").gt(50000))
                .select(
                        col("EmpID"),
                        col("Name"),
                        col("DeptID"),
                        col("Salary").alias("AnnualSalary")
                );

        System.out.println("--- Transformed Data ---");
        transformedDF.show();

        transformedDF.write()
                .mode("overwrite")
                .parquet("src/main/resources/data/employees_parquet");

        System.out.println("ETL Pipeline completed successfully!");
    }

}
