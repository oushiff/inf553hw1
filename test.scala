/**
  * Created by Franz on 9/18/16.
  */




import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.parsing.json._



object Sentiment {
  def main(arg: Array[String]) {


    val sparkConf = new SparkConf().setAppName("Sentiment_score")
    val sc = new SparkContext(sparkConf)

    val textTwitter = sc.textFile(arg(0))
    val textAFINN = sc.textFile(arg(1))


    val counts = textTwitter.foreach {
      JSON.parseFull(_).filter( key => key == "text")
    }
    //val aaa = JSON.parseFull()

    //val text = scala.io.Source.fromFile(arg(0), "UTF-8").toString().toLowerCase().split(" ").map(word => (word, 1))

    textTwitter.saveAsTextFile("testsave.txt")

  }
}