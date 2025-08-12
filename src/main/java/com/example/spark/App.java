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

//        question13 obj13 = new question13();
//        obj13.co_Occurences(spark);

//        question14 obj14 = new question14();
//        obj14.runningTotal(spark);

//        question15 obj15 = new question15();
//        obj15.CsvToParquet(spark);
//
//        question16 obj16 = new question16();
//        obj16.filterNullEmptyVal(spark);

//        question17 obj17 = new question17();
//        obj17.pivot(spark);

//        question18 obj18 = new question18();
//        obj18.unPivot(spark);

//        question19 obj19 = new question19();
//        obj19.joins(spark);

//        question20 obj20 = new question20();
//        obj20.movingfAvg(spark);

//        question21 obj21 = new question21();
//        obj21.etlPipeline(spark);

//        question22 obj22 = new question22();
//        obj22.tokenize(spark);

//        question23 obj23 = new question23();
//        obj23.correlation(spark);

//        question24 obj24 = new question24();
//        obj24.filter(spark);

//        question25 obj25 = new question25();
//        obj25.repartitionAndCoalesce(spark);


        spark.stop();

    }
}
