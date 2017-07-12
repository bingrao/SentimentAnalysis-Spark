package org.ucf.spark.SentimentAnalysis

object MLSentimentAnalyzer {

  def normalizeMLSentiment(sentiment: Double) = {
    sentiment match {
      case x if x < 0   => 0 // negative
      case x if x == 0  => 1 // neutral
      case x if x > 0   => 2 // positive
      case _ => 0 // if cannot figure the sentiment, term it as neutral
    }
  }	
	/**
    * Strips the extra characters in tweets. And also removes stop words from the tweet text.
    *
    * @param tweetText     -- Complete text of a tweet.
    * @param stopWordsList -- Broadcast variable for list of stop words to be removed from the tweets.
    * @return Seq[String] after removing additional characters and stop words from the tweet.
  */
  def getBarebonesTweetText(tweetText:String): Seq[String] = {
    //Remove URLs, RT, MT and other redundant chars / strings from the tweets.
    tweetText.toLowerCase()
      .replaceAll("\n", "")
      .replaceAll("rt\\s+", "")
      .replaceAll("\\s+@\\w+", "")
      .replaceAll("@\\w+", "")
      .replaceAll("\\s+#\\w+", "")
      .replaceAll("#\\w+", "")
      .replaceAll("(?:https?|http?)://[\\w/%.-]+", "")
      .replaceAll("(?:https?|http?)://[\\w/%.-]+\\s+", "")
      .replaceAll("(?:https?|http?)//[\\w/%.-]+\\s+", "")
      .replaceAll("(?:https?|http?)//[\\w/%.-]+", "")
      .split("\\W+")
      .filter(_.matches("^[a-zA-Z]+$"))
    //.filter(!stopWordsList.contains(_))
    //.fold("")((a,b) => a.trim + " " + b.trim).trim
  }
	
}