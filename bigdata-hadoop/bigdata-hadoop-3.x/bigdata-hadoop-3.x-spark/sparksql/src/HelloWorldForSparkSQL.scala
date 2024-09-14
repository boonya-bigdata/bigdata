

import org.apache.spark.{ SparkConf, SparkContext }

case class Person(name: String, age: Int)

object HelloWorldForSparkSQL {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val people = sc.textFile("file:///root/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))

    val df = sqlContext.createDataFrame(people)

    df.registerTempTable("people")

    val teenagers = sqlContext.sql("SELECT * FROM people WHERE age >= 10 AND age <= 19")
    teenagers.collect().foreach(println)

  }
}
