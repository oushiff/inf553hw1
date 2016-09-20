/**
  * Created by Franz on 9/19/16.
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.parsing.json._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object TFDF {

  // var tempTextFile = RDD

  def main(arg: Array[String]): Unit = {
    test(Array("shortTwitter.txt","fewfw"))
  }


  def wordStandize(word: String): String = {
    val trimedWord = word.trim
    val firstChar = trimedWord.charAt(0)
    if (firstChar == '@' || firstChar == '#')
      return ""
    var lowerWord = trimedWord.toLowerCase
    if (lowerWord.contains("://") || lowerWord.contains("rt@") || lowerWord == "rt")
      return  ""

    return """^\W+|\W+$""".r replaceAllIn(lowerWord, "")
  }

  def toUTF (word: String): String = {
    return java.net.URLEncoder.encode(word.replaceAll("\\p{C}", ""), "utf-8")
  }

  //  def getTF(line: String): Unit = {
  //    val words:Array[String] = line.split(" ")
  //    var rate = 0
  //    var tfRes: Array[(String)] = Array()
  //
  //    for ( word <- words ) {
  //      tfRes += (wordStandize(word), 1)
  //    }
  //
  //    return tfRes
  //  }
  //
  //  def getTF(line: String, sc: SparkContext): Unit = {
  //    val words:Array[String] = line.split(" ")
  //    var rate = 0
  //
  //    val tempTextFile = sc.parallelize(words)
  //
  //    tempTextFile
  //      .map(word => (wordStandize(word), 1))
  //      .countByKey()
  //
  //    return tempTextFile
  //  }

  //  def my_reduce(value1:(String,Long,Int), value2:(String,Long,Int)): (String,Long,Int) = {
  //
  //    if (value1._2 == value2._2 && value1._1 == value2._1)
  //      return (value1._1, value1._2, value1._3 + value2._3 )
  //  }

  def test(arg: Array[String]) {



    val sparkConf = new SparkConf().setAppName("TFDF").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val textTwitter = sc.textFile(arg(0))




    val lines = textTwitter
      .flatMap(line=>JSON.parseFull(line).get.asInstanceOf[Map[String,String]].get("text"))
      .zipWithIndex()
      .flatMap ( input => {
        input._1.split(" ")
          .map(word => ((toUTF(wordStandize(word)),input._2+1),1))
      } )
      .filter(input=>input._1._1.length > 0)
      .reduceByKey(_ + _)
      .map(input => (input._1._1, Array((input._1._2, input._2))))
      .reduceByKey(_ ++ _)
      .map(input=>(input._1, input._2.length, input._2.toList.sorted))
      .sortBy(r=>r._1)

    //.map(word=>wordStandize(word))

    lines.foreach(println)

    //    for( (line, index) <- lines.zipWithIndex()) {
    //      val words: Array[String] = line.split(" ")
    //      var tempTextFile = sc.parallelize(words)
    //      tempTextFile
    //        .map(word=>wordStandize(word))
    //      //      val tempTextFile = getTF(line, sc)
    //      //      println(tempTextFile)
    //      tempTextFile.foreach(println)
    //    }
    //    //.map(line =>  getRate(line))
    //

    //.map(line=>line.split(" ").toString().toLowerCase().map(lowerWord=>sentiMap.getOrElse(lowerWord.toString(),"0")))



    //   println(counts)

    //words.foreach(println)


    //val aaa = JSON.parseFull()

    //val text = scala.io.Source.fromFile(arg(0), "UTF-8").toString().toLowerCase().split(" ").map(word => (word, 1))

    // counts.saveAsTextFile("testsave.txt")



  }
}