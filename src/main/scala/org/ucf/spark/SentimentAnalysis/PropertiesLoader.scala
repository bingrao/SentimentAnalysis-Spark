package org.ucf.spark.SentimentAnalysis

import com.typesafe.config.{Config, ConfigFactory}
import java.io.File
object PropertiesLoader {
	//private val conf:Config = ConfigFactory.load("application.conf")
	
	val parsedConfig = ConfigFactory.parseFile(new File("/home/bing/command/spark/sentiment/application.conf"))
  private val conf = ConfigFactory.load(parsedConfig)
	
	val dbURL = conf.getString("DBURL")
	val dbUser = conf.getString("DBUSER")
	val dbPasswd = conf.getString("DBPASSWD")
	
	val numFeatures = conf.getInt("NUMFEATURES")
	val numClass = conf.getInt("NUMCLASSES")
	
	val numIterater = conf.getInt("NUMITERATION")
}