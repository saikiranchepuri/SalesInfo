package com.epam.salestest

import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException
import scala.io.Source


class SalesTest extends FunSuite {

	val spark = SparkSession.builder().master("yarn").appName("salesInfo").getOrCreate()
 
    	
	test("mocking file contents"){
    val input = Source.fromFile("src/main/resources/Downloads/salesMock.txt").mkString
    val expected = spark.sparkContext.parallelize(Seq(List(3763645,PBS Benefits and ServicesALECENSA LUNG TARGETED#S#NSWS#NSW-011226W011226W#20180201,2/1/2018,31136,45)))
    assert(input == expected)
    }
	
	test("mocking for year"){
		val year = Sales.yr
		val expected = spark.sparkContext.parallelize(Seq(List(3763645,PBS Benefits and ServicesALECENSA LUNG TARGETED#S#NSWS#NSW-011226W011226W#20180201,2/1/2018,31136,45,2018)))
		assert(year == expected)
	}
	
	test("mocking for quarter"){
		val quarter = Sales.Qtr
		val expected = spark.sparkContext.parallelize(Seq(List(3763645,PBS Benefits and ServicesALECENSA LUNG TARGETED#S#NSWS#NSW-011226W011226W#20180201,2/1/2018,31136,45,1)))
		assert(quarter == expected)
	}
	
	test("Test failure scenario for Year"){
     val year = Sales.yr
	 val expected = spark.sparkContext.parallelize(Seq(List(3763645,PBS Benefits and ServicesALECENSA LUNG TARGETED#S#NSWS#NSW-011226W011226W#20180201,2/1/2018,31136,45)))
	 assertThrows[TestFailedException]{
		assert(year == expected)
	 }
	 	
	}
	
	test("Test failure scenario for Quarter"){
		val quarter = Sales.Qtr
		val expected = spark.sparkContext.parallelize(Seq(List(3763645,PBS Benefits and ServicesALECENSA LUNG TARGETED#S#NSWS#NSW-011226W011226W#20180201,2/1/2018,31136,45)))
		assertThrows[TestFailedException]{
			assert(quarter == expected)
		}
	}
}
	
	
}
