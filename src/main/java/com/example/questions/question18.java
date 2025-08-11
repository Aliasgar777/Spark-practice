package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class question18 {

//    18. Implement a Java Spark job to unpivot a wide dataset into long format.

    public void unPivot(SparkSession spark){
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/pivoted_retail_sales.csv");

        Dataset<Row> unpivotDf = df.selectExpr(
                "Store",
                "Product",
                "stack(2, 'Jan', Jan, 'Feb', Feb) as (Month, Sale)"
        );

        unpivotDf.show();
    }

}
