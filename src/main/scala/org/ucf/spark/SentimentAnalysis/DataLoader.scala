package org.ucf.spark.SentimentAnalysis
import java.util.Properties
import org.apache.spark.sql.{DataFrame,Row,Column,SparkSession}


object DataLoader {	
  private val url = PropertiesLoader.dbURL
  private val prop = new Properties()
  prop.setProperty("user", PropertiesLoader.dbUser)
  prop.setProperty("password", PropertiesLoader.dbPasswd)
  prop.setProperty("driver", "com.mysql.jdbc.Driver")
  
	/**
	 * load all data from MySQL database
	 */
	def loadDataFromMySQL(table:String,spark:SparkSession):DataFrame = spark.read.jdbc(url,table,prop)
	
	/**
	 * Load the data which satisfy the condition sql from MySQL database
	 */
	def loadDataFromMySQL(table:String,
	                      spark:SparkSession,
	                      sql:String):DataFrame = 
	  spark.read.jdbc(url,table,Array(sql),prop)

	def loadDataFromText(path:String,spark:SparkSession):DataFrame = spark.read.text(path)
}