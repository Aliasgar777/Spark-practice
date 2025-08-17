package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

public class question26 {

//    26. Create a Java Spark job to cache and persist a dataset and observe performance.

    public void cacheAndPersist(SparkSession spark){
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/employees.csv");

        System.out.println("Dataset schema:");
        df.printSchema();

        Dataset<Row> transformed = df.filter("Salary > 50000")
                .select("Name",  "Salary");

        long start1 = System.currentTimeMillis();
        System.out.println("Count without cache: " + transformed.count());
        long end1 = System.currentTimeMillis();
        System.out.println("Time without cache: " + (end1 - start1) + " ms");

        transformed.cache();

        long start2 = System.currentTimeMillis();
        System.out.println("Count with cache: " + transformed.count());
        long end2 = System.currentTimeMillis();
        System.out.println("Time with cache: " + (end2 - start2) + " ms");

        transformed.persist(StorageLevel.MEMORY_AND_DISK());

        long start3 = System.currentTimeMillis();
        System.out.println("Count with persist: " + transformed.count());
        long end3 = System.currentTimeMillis();
        System.out.println("Time with persist: " + (end3 - start3) + " ms");

        transformed.unpersist();
    }
}
