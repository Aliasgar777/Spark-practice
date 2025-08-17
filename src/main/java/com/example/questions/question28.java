package com.example.questions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

public class question28 {

//    28. Write a Java Spark job to implement accumulators for counting specific events.
    public void accumulator(SparkSession spark){

        Dataset<Row> employeeDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/employees.csv");

        employeeDf.show();

        LongAccumulator deptCount = spark.sparkContext().longAccumulator("Department count");
        LongAccumulator salaryCount = spark.sparkContext().longAccumulator("Salary");

        employeeDf.foreach(row -> {
            int dept = row.getAs("DeptID");
            int salary = row.getAs("Salary");

            if (dept == 101) {
                deptCount.add(1);
            }
            if (salary > 50000) {
                salaryCount.add(1);
            }
        });

        System.out.println("-------------------");
        System.out.println("Number of employees from dept 101: " + deptCount.value());
        System.out.println("-------------------");
        System.out.println("Number of employees with salary > 50,000: " + salaryCount.value());
        System.out.println("-------------------");

    }
}
