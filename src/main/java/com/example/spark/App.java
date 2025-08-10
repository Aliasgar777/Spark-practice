package com.example.spark;


import com.example.questions.*;
import org.apache.spark.sql.SparkSession;

public class App
{
    public static void main( String[] args ) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark Project")
                .master("local[*]")
                .getOrCreate();

//        question1 obj = new question1();
//        obj.countWords(spark);

//        question2 obj2 = new question2();
//        obj2.filterLinesByKeyword(spark);

//        question3 obj3 = new question3();
//        obj3.countChar(spark);

//        question4 obj4 = new question4();
//        obj4.printSchema(spark);

//        question5 obj5 = new question5();
//        obj5.avgMinMax(spark);

//        question6 obj6 = new question6();
//        obj6.joinDataset(spark);

//        question7 obj7 = new question7();
//        obj7.countRows(spark);

//        question8 obj8 = new question8();
//        obj8.removeDuplicates(spark);

//        question9 obj9 = new question9();
//        obj9.sortByColumn(spark);

//        question10 obj10 = new question10();
//        obj10.loadJson(spark);

//        question12 obj12 = new question12();
//        obj12.top5Words(spark);




        spark.stop();

    }
}
