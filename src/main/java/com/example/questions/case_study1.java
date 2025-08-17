package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class case_study1 {
    public void sales(SparkSession spark){
        Dataset<Row> ordersDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/orders.csv");

        Dataset<Row> usersDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/users.csv");

        ordersDF.printSchema();
        usersDF.printSchema();

        Dataset<Row> ordersWithRevenue = ordersDF.withColumn("revenue",
                col("quantity").multiply(col("price")));

        Dataset<Row> productSales = ordersWithRevenue.groupBy("product_id")
                .agg(
                        sum("revenue").alias("total_revenue"),
                        sum("quantity").alias("total_quantity")
                )
                .orderBy(col("total_revenue").desc());

        Dataset<Row> top10Products = productSales.limit(10);

        System.out.println("Top 10 Products by Revenue:");
        top10Products.show(false);

        Dataset<Row> revenueByLocation = ordersWithRevenue
                .join(usersDF, "user_id")
                .groupBy("location")
                .agg(sum("revenue").alias("total_revenue"))
                .orderBy(col("total_revenue").desc())
                .limit(5);

        System.out.println("Top 5 Locations by Revenue:");
        revenueByLocation.show(false);

        Dataset<Row> userOrders = ordersDF.groupBy("user_id")
                .agg(countDistinct("order_id").alias("order_count"));

        Dataset<Row> repeatCustomers = userOrders
                .filter(col("order_count").gt(5))
                .join(usersDF, "user_id")
                .select("user_id", "user_name", "order_count", "location")
                .orderBy(col("order_count").desc());

        System.out.println("Repeat Customers (>5 orders):");
        repeatCustomers.show(false);

        productSales.write().mode("overwrite").parquet("output/product_sales");
        revenueByLocation.write().mode("overwrite").parquet("output/location_revenue");
        repeatCustomers.write().mode("overwrite").parquet("output/repeat_customers");
    }
}
