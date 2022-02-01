import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, mean, round, when}

object main extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark Projet")
    .master("local[*]")
    .getOrCreate()

  val myData = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/data/US_Accidents_Dec20_updated.csv")

  myData.show()
  val myDataTemp = myData.select("ID","Severity","Temperature(F)","Wind_Chill(F)")
  myDataTemp
    .show()
  val dataTempMeanValue = myDataTemp.filter(col("Wind_Chill(F)").isNotNull)
    .groupBy("Severity")
    .agg(mean("Wind_Chill(F)").as("avg"))
    .withColumn("avg",round(col("avg"),1))
  dataTempMeanValue.show()

  val myNewDataTemp = myDataTemp
    .join(dataTempMeanValue,"Severity")
    .withColumn("Wind_Chill(F)",when(col("Wind_Chill(F)").isNull,col("avg")).otherwise(col("Wind_Chill(F)")))
    .drop("avg")

  myNewDataTemp.filter(col("Wind_Chill(F)").isNull).show()



}
