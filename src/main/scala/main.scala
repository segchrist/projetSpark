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
  val dataWindChillMeanValue = myDataTemp.filter(col("Wind_Chill(F)").isNotNull)
    .groupBy("Severity")
    .agg(mean("Wind_Chill(F)").as("avg1"))
    .withColumn("avg1",round(col("avg1"),1))
  dataWindChillMeanValue.show()

  val dataTempMeanValue = myDataTemp.filter(col("Temperature(F)").isNotNull)
    .groupBy("Severity")
    .agg(mean("Temperature(F)").as("avg2"))
    .withColumn("avg2",round(col("avg2"),1))
  dataTempMeanValue.show()

  val myNewDataTemp = myDataTemp
    .join(dataWindChillMeanValue,"Severity")
    .withColumn("Wind_Chill(F)",when(col("Wind_Chill(F)").isNull,col("avg1")).otherwise(col("Wind_Chill(F)")))
    .drop("avg1")
    .join(dataTempMeanValue,"Severity")
    .withColumn("Temperature(F)",when(col("Temperature(F)").isNull,col("avg2")).otherwise(col("Temperature(F)")))
    .drop("avg2")

  myNewDataTemp.filter(col("Temperature(F)").isNull)show()

  //Nombre d'accidents par gravité pour chaque température

  val dataCountPerTemp = myNewDataTemp.groupBy("Severity","Temperature(F)").count().orderBy("count")
  dataCountPerTemp.show()

  //Nombre d'accidents par gravité pour chaque wind_chill

  val dataCountPerWindChill = myNewDataTemp.groupBy("Severity","Wind_Chill(F)").count().orderBy("count")
  dataCountPerWindChill.show()

  //Moyenne des températures par  sévérité

  val dataMeanTemPerSev = myDataTemp
    .groupBy("Severity")
    .agg(mean("Temperature(F)").as("Temp_Mean"))
    .withColumn("Temp_Mean",round(col("Temp_Mean"),1))
    .orderBy("Severity")

  val dataMeanWindChillPerSev = myDataTemp
    .groupBy("Severity")
    .agg(mean("Wind_Chill(F)").as("Wind_Chill_Mean"))
    .withColumn("Wind_Chill_Mean",round(col("Wind_Chill_Mean"),1))
    .orderBy("Severity")

  dataMeanWindChillPerSev.show()
  dataMeanTemPerSev.show()





}
