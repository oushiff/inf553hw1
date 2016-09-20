/**
  * Created by Franz on 9/18/16.
  */




import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.parsing.json._



object Sentiment {

  def main(arg: Array[String]): Unit = {
    test(Array("fei_fan_first20.txt", "AFINN-111.txt"))
  }


  def getText(line: String): Int = {
    val words:Array[String] = line.split(" ")

    val rate = 0

    for ( word <- words ) {
      if (word.toLowerCase().eq("dd")) {
        
      }
      //val lowercase = word.toLowerCase()
      //println(lowercase)
    }

    return 1
  }


  def test(arg: Array[String]) {


    val sparkConf = new SparkConf().setAppName("Sentiment_score").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val textTwitter = sc.textFile(arg(0))
    val textAFINN = sc.textFile(arg(1))


//    val counts = textTwitter
//      .map(line=>JSON.parseFull(line))

    val counts = textTwitter
      .flatMap(line=>JSON.parseFull(line).get.asInstanceOf[Map[String,String]].get("text"))
      .map(line => getText(line))
      //.map(line=>line.split(" ").toString().toLowerCase())


    println(counts.count())



    //val aaa = JSON.parseFull()

    //val text = scala.io.Source.fromFile(arg(0), "UTF-8").toString().toLowerCase().split(" ").map(word => (word, 1))

    //counts.saveAsTextFile("testsave.txt")



  }
}