import org.apache.spark.sql.SparkSession

object main extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark Projet")
    .master("local[*]")
    .getOrCreate()

  val myData = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("US_Accidents_Dec20_updated.csv")

  val newData = myData
}
