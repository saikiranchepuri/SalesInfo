package com.epam.sales

import org.apache.spark.sql.SparkSession

trait Configuration {
  val spark = SparkSession.builder().master("yarn").appName("SalesInfo").getOrCreate()
}
