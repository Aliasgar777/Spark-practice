package com.example.spark;


import com.example.questions.question1;
import org.apache.spark.sql.SparkSession;

public class App
{
    public static void main( String[] args ) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark Project")
                .master("local[*]")
                .getOrCreate();

        question1 obj = new question1();

        obj.countWords(spark);

        spark.stop();

    }
}
