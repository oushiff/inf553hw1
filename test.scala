/**
  * Created by Franz on 9/18/16.
  */




import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.parsing.json._
import org.apache.spark.rdd.RDD
import scala.io.Source
//import scala.collection.mutable.Map



object Sentiment {

  var sentiMap = collection.mutable.Map[String, String]()

  def main(arg: Array[String]): Unit = {
    test(Array("fei_fan_first20.txt", "AFINN-111.txt"))
  }


  def wordStandize(word: String): String = {
    val trimedWord = word.trim
    val firstChar = trimedWord.charAt(0)
    if (firstChar == '@' || firstChar == '#')
      return "   "
    var lowerWord = trimedWord.toLowerCase
    if (lowerWord.contains("://") || lowerWord.contains("rt@"))
      return  "   "
    while (lowerWord.length() > 0) {
      if (lowerWord.charAt(lowerWord.length() - 1) > 'z' || lowerWord.charAt(lowerWord.length() - 1) < 'a') {
        lowerWord = lowerWord.substring(0, lowerWord.length() - 1)
      }
      else {
        return lowerWord
      }

    }
    return lowerWord
  }


  def getRate(line: String): Int = {
    val words:Array[String] = line.split(" ")
    var rate = 0
    for ( word <- words ) {
      rate += sentiMap.getOrElse(wordStandize(word), "0").toInt
    }
    return rate
  }


  def test(arg: Array[String]) {


    val sparkConf = new SparkConf().setAppName("Sentiment_score").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val textTwitter = sc.textFile(arg(0))
    val textAFINN = sc.textFile(arg(1))


    for (line <- textAFINN) {
      val tokens = line.split("\t")
      sentiMap(tokens(0).trim) = tokens(1).trim

    }


    //sentiMap.foreach(println)



    val lines = textTwitter
      .flatMap(line=>JSON.parseFull(line).get.asInstanceOf[Map[String,String]].get("text"))

    for( (word, index) <- lines.zipWithIndex()) {
      println(index + 1, getRate(word))
    }
        //.map(line =>  getRate(line))


    //.map(line=>line.split(" ").toString().toLowerCase().map(lowerWord=>sentiMap.getOrElse(lowerWord.toString(),"0")))



 //   println(counts)

    //words.foreach(println)


    //val aaa = JSON.parseFull()

    //val text = scala.io.Source.fromFile(arg(0), "UTF-8").toString().toLowerCase().split(" ").map(word => (word, 1))

   // counts.saveAsTextFile("testsave.txt")



  }
}