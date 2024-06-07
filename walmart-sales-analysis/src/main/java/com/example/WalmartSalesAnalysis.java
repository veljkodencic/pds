package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class WalmartSalesAnalysis {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Walmart Sales Analysis")
                .master("local[*]")
                .getOrCreate();

        // Učitavanje podataka
        Dataset<Row> salesData = spark.read()
                .option("header", "true")
                .csv("src/main/resources/walmart.csv");

        // Transformacije podataka - konverzija kolona u odgovarajuće tipove
        salesData = salesData.withColumn("Purchase", salesData.col("Purchase").cast("double"))
                .withColumn("Age", functions.split(salesData.col("Age"), "-"))
                .withColumn("Age", functions.expr("CAST((int(Age[0]) + int(Age[1])) / 2 AS INT)"));

        // Realizacija upita
        query1(spark, salesData);
        query2(spark, salesData);
        query3(spark, salesData);
        query4(spark, salesData);

        spark.stop();
    }

    private static void query1(SparkSession spark, Dataset<Row> salesData) {
        // Upit 1
        Dataset<Row> result = salesData.groupBy("City_Category", "Occupation", "Marital_Status")
                .agg(functions.avg("Purchase").alias("Average_Purchase"))
                .filter("Marital_Status = '0'")
                .orderBy(functions.desc("Average_Purchase"));

        result.show();
    }

    private static void query2(SparkSession spark, Dataset<Row> salesData) {
        // Upit 2
        Dataset<Row> result = salesData.groupBy("Product_ID", "Age", "City_Category")
                .agg(functions.count("Purchase").alias("Purchase_Count"))
                .orderBy(functions.desc("Purchase_Count"));

        result.show();
    }

    private static void query3(SparkSession spark, Dataset<Row> salesData) {
        // Upit 3
        Dataset<Row> result = salesData.groupBy("Product_ID", "Occupation", "Gender")
                .agg(functions.count("Purchase").alias("Purchase_Count"))
                .orderBy(functions.desc("Purchase_Count"));

        result.show();
    }

    private static void query4(SparkSession spark, Dataset<Row> salesData) {
        // Upit 4
        Dataset<Row> result = salesData.filter("Marital_Status = '0'")
                .groupBy("Age", "Gender", "City_Category")
                .agg(functions.sum("Purchase").alias("Total_Purchase"))
                .orderBy(functions.desc("Total_Purchase"));

        result.show();
    }
}
