package com.epam.sales

import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.functions.quarter

object Sales extends Configuration with App {
  val logger = Logger.getLogger(getClass.getName)
  
  val salesPath = args(0) // hdfs://nn01.itversity.com/user/saikiranvishu/fact_sales/
  
 
  val factData = spark.read.option("header","false").format("csv").load(args(0))
  val header = factData.first
  val rows = factData.filter(l=>l!=header)
  val factDF = rows.toDF("sales_fact_id","sales_fact_key","period","gross_sale","units")
    
  // Extracting Year 
  val yr  = factDF.withColumn("period",col("period").cast(DateType)).withColumn("gross_sale",col("gross_sale").cast(IntegerType)).withColumn("units",col("units").cast(IntegerType)).withColumn("YEAR",year(to_timestamp($"period","yyyy-MM-dd")))
  
 // For the year 2019: 
 val y = yr.select(col("gross_sale")).where(col("YEAR")===2019)
 
 // For the year 2018: 
 val x = yr.select(col("gross_sale")).where(col("YEAR")===2018)
 
 Quater extraction from period column:
 val quarter = factDF.withColumn("period",col("period").cast(DateType)).withColumn("gross_sale",col("gross_sale").cast(IntegerType)).withColumn("units",col("units").cast(IntegerType)).withColumn("Quater",quarter(to_timestamp($"period","yyyy-MM-dd")))

 s val Qtr = df.withColumn("QTR",quarter($"period"))
 
 // Now, directly find the quarter sales:
 val q2019 = Qtr.select(col("gross_sale")).where(col("QTR")===1)
 val q2019 = Qtr.select(col("gross_sale")).where(col("QTR")===2)
 val q2019 = Qtr.select(col("gross_sale")).where(col("QTR")===3)
 val q2019 = Qtr.select(col("gross_sale")).where(col("QTR")===4)
 
 val q2018 = Qtr.select(col("gross_sale")).where(col("QTR")===1)
 val q2018 = Qtr.select(col("gross_sale")).where(col("QTR")===2)
 val q2018 = Qtr.select(col("gross_sale")).where(col("QTR")===3)
 val q2018 = Qtr.select(col("gross_sale")).where(col("QTR")===4)
 
 /* Since, the Quater() function returns a number from 1 to 4
 from Jan-March returns 1
 April - June returns 2
 July - Sept returns 3
 Oct - Dec returns 4  */
   
} 
  
  
 
