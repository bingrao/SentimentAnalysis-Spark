package org.ucf.spark.SentimentAnalysis
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession,Row,DataFrame}
import org.apache.spark.ml.feature.{HashingTF,Word2Vec}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object SentimentAnalysisFactory {
	def main(args: Array[String]) {
    val numFArray = Array(512,1024,2048,4096,8192,16384)
    val endDate = Array("2013-05-25 00:00:00",
                        "2013-06-05 00:00:00",
                        "2013-06-15 00:00:00",
                        "2013-06-25 00:00:00",
                        "2013-07-05 00:00:00",
                        "2013-07-15 00:00:00")
    var nFeatures = 0
    var endTime = "2013-05-25 00:00:00"
    val startTime = "2013-05-21 00:00:00"
    if(args.length == 2){
      nFeatures = numFArray.apply(args(0).toInt)
      endTime = endDate.apply(args(1).toInt)
    }else{
      println("The input parameter wrong, please double check")
      System.exit(-1)
    }
    
//    val nFeatures = PropertiesLoader.numFeatures
	  val nClass = PropertiesLoader.numClass
	  val nIterator = PropertiesLoader.numIterater
    
    val spark = SparkSession.builder()
	                          .appName(this.getClass.getSimpleName)
	                          .config("spark.serializer", classOf[KryoSerializer].getCanonicalName)
	                          .config("spark.kryoserializer.buffer.max", 512)
	                          .getOrCreate()
    val sc = spark.sparkContext  
                        
	  val sql = "Created < '"+endTime+"' and Created > '"+ startTime+"'"
	  val raw = DataLoader.loadDataFromMySQL("message",spark,sql)
	                          .select("MsgSentiment","Content")
	                          .rdd
	                          .map(
	                              row => (MLSentimentAnalyzer.normalizeMLSentiment(row.getDouble(0)),
	                                      MLSentimentAnalyzer.getBarebonesTweetText(row.getString(1)))
	                          ).repartition(50)
	  val rawData =  raw.cache()                       
	  
	  println("numFeatures: "+nFeatures)
	  println("raw data size: "+rawData.count())
	  
	  naiveBayesModel("HashingTF")
//	  naiveBayesModel("Word2Vec")
	  neuralNetwokModel("HashingTF")
//	  neuralNetwokModel("Word2Vec")
	  spark.stop()
	  
	  
	  def naiveBayesModel(select:String) = {
	   import spark.implicits._
	   import org.apache.spark.ml.classification.NaiveBayes
	   val data = rawData.toDF("label","message")
	   var dataset:DataFrame = null
	   select match {
	     case "HashingTF" => {
	        val tf = new HashingTF().setNumFeatures(nFeatures)
                              .setInputCol("message")
                              .setOutputCol("features")
          dataset = tf.transform(data)
	     }
	     case "Word2Vec" => {
	        val word2Vec = new Word2Vec().setInputCol("message")
	                                 .setOutputCol("features")
	                                 .setVectorSize(nFeatures)
	                                 .setMaxIter(nIterator)
	                                 .fit(data)
	                                 
	        dataset =  word2Vec.transform(data)
	     }
	     case _ => {
	      println("Wrong selection, pelase double check"+ select)
	      System.exit(-1)
	     }
	   }
	   //println("Here we use "+select+" to extract feature from raw data naiveBayesModel")
	   val Array(training, test) = dataset.randomSplit(Array(0.6, 0.4),seed = 1234L)
	   val model = new NaiveBayes().setFeaturesCol("features")
	                                .setLabelCol("label")
	                                .fit(training)
	                                
	   val predictions = model.transform(test)
	    
	   val evaluator = new MulticlassClassificationEvaluator()
	                        .setLabelCol("label")
	                        .setPredictionCol("prediction")
	                        .setMetricName("accuracy")
	                                         
	   val accuracy = evaluator.evaluate(predictions)
     println("naiveBayesModelWithTFIDF Accuracy: " + accuracy)
	   
	  }
	  
	  def neuralNetwokModel(select:String) = {
	   import spark.implicits._
	   import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
	   val data = rawData.toDF("label","message")
	   var dataset:DataFrame = null
	   select match {
	     case "HashingTF" => {
	        val tf = new HashingTF().setNumFeatures(nFeatures)
                              .setInputCol("message")
                              .setOutputCol("features")
          dataset = tf.transform(data)
	     }
	     case "Word2Vec" => {
	        val word2Vec = new Word2Vec().setInputCol("message")
	                                 .setOutputCol("features")
	                                 .setVectorSize(nFeatures)
	                                 .setMaxIter(nIterator)
	                                 .fit(data)
	                                 
	        dataset =  word2Vec.transform(data)
	     }
	     case _ => {
	      println("Wrong selection, pelase double check"+ select)
	      System.exit(-1)
	     }
	   }
	   //println("Here we use "+select+" to extract feature from raw data neuralNetwokModel")
	   
	    // Split data into training (60%) and test (40%).
      val Array(training, test) = dataset.randomSplit(Array(0.6, 0.4),seed = 1234L)
      
	     // specify layers for the neural network:
      // input layer of size nFeatures (features), two intermediate of size 5 and 4
      // and output of size 3 (classes)
      val layers = Array[Int](nFeatures,5,4,nClass)
      // create the trainer and set its parameters
      val model = new MultilayerPerceptronClassifier()
        .setLayers(layers)
        .setBlockSize(128)
        .setSeed(1234L)
        .setMaxIter(nIterator)
        .fit(training)
        
      // compute accuracy on the test set
      val predictions = model.transform(test)
	    
	    val evaluator = new MulticlassClassificationEvaluator()
	                        .setLabelCol("label")
	                        .setPredictionCol("prediction")
	                        .setMetricName("accuracy")
	                                         
	    val accuracy = evaluator.evaluate(predictions)
      println("neuralNetwokModel Accuracy: " + accuracy)
	  }
	}
}