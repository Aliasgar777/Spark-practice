package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class question11 {

//    11. Implement a Java Spark job to calculate total sales per product from a sales dataset.

    public void salesPerProduct(SparkSession spark){
        Dataset<Row> df = spark.read()
                .option("multiline", "true")
                .option("inferSchema", "true")
                .json("src/main/resources/data/sales.json");

        Dataset<Row> sales = df.withColumn("sale", col("quantity").multiply(col("price")))
                .groupBy("product")
                .agg(sum("sale").alias("total_sale"))
                .orderBy(desc("total_sale"));

        sales.show();
    }
}
