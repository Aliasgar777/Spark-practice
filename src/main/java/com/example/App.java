package com.example;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class App
{
    public static void main( String[] args ) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark Project")
                .master("local[*]")
                .getOrCreate();

//        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//
//        JavaRDD<String> lines = sc.textFile("src/main/resources/data/people.csv");
//        String header = lines.first();
//
//        JavaRDD<String> data = lines.filter(line -> !line.equals(header));
//
//        JavaRDD<String[]> splitData = data.map(line -> line.split(","));
//
//        JavaRDD<String[]> filtered = splitData.filter(arr -> Integer.parseInt(arr[2]) > 28);
//
//        JavaRDD<String> nameCity = filtered.map(arr -> arr[1] + " from " + arr[3]);
//
//        JavaRDD<String> distinctNameCity = nameCity.distinct();
//
//        distinctNameCity.collect().forEach(System.out::println);

//        sc.close();

        Dataset<Row> df = spark.read()
                            .option("header", "true")
                                .option("inferSchema", "true")
                                    .csv("src/main/resources/data/people.csv");
//        df.printSchema();
//        df.select("name").show();
//        df.filter(col("age").gt(21)).show();
//        df.groupBy("age").count().show();

        df.createOrReplaceTempView("people");
        Dataset<Row> nameAndAge = spark.sql("SELECT name, age FROM people");

        nameAndAge.show();
        spark.stop();

    }
}
