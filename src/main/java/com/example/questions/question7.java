package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class question7 {

//     7. Implement a Java Spark job to group data by a column and count rows per group.

    public void countRows(SparkSession spark){
        Dataset<Row> people_df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/people.csv");

        Dataset<Row> joined = people_df.groupBy("city").count()
                        .withColumnRenamed("count", "employees");

        joined.show();
    }

}
