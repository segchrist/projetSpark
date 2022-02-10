import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, desc, mean, round, sum, when}

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
/*
  val dataWeatherCond = myData.select("ID","Severity","Weather_Condition")
  dataWeatherCond.filter(col("Weather_Condition").isNull).show()
  val dataMCVWeatherCond = dataWeatherCond.filter(col("Weather_Condition").isNotNull)
    .groupBy("Severity","Weather_Condition")
    .count().orderBy(col("count").desc)
    .withColumn("Weather_Condition",)
  dataMCVWeatherCond.show()*/
   /*val newDataWeatherCond = dataWeatherCond.join(dataMCVWeatherCond,"Severity")
    .withColumn("Weather_Condition",when(col("Weather_Condition").isNull,col("count")).otherwise(col("Wind_Chill(F)")))
*/

  val myNewData = myData
    .select("ID","Severity","Temperature(F)","Wind_Chill(F)","Humidity(%)","Pressure(in)" ,
      "Visibility(mi)", "Wind_Direction","Wind_Speed(mph)","Precipitation(in)","Weather_Condition","Sunrise_Sunset")
    .filter(col("Wind_Direction").isNotNull)
    .filter(col("Weather_Condition").isNotNull)
    .filter(col("Sunrise_Sunset").isNotNull)
    .withColumn("Temperature(F)",round((col("Temperature(F)")-32)/1.8,1))
    .withColumn("Wind_Chill(F)",round((col("Wind_Chill(F)")-32)/1.8,1))
    .withColumn("Visibility(mi)", round(col("Visibility(mi)")*1.6))
    .withColumn("Wind_Speed(mph)",round(col("Wind_Speed(mph)")*1.6))
    .withColumnRenamed("Wind_Chill(F)","Wind_Chill")
    .withColumnRenamed("Temperature(F)","Temperature")
    .withColumnRenamed("Humidity(%)","Humidity")
    .withColumnRenamed("Pressure(in)","Pressure")
    .withColumnRenamed("Visibility(mi)","Visibility")
    .withColumnRenamed("Wind_Speed(mph)","Wind_Speed")
    .withColumnRenamed("Precipitation(in)","Precipitation")

  myNewData.show()


  val dataAvgWindChill = myNewData
    .filter(col("Wind_Chill").isNotNull)
    .groupBy(col("Severity"))
    .agg(avg("Wind_Chill"))
    .withColumnRenamed("avg(Wind_Chill)","avg_Wind_Chill")
    .withColumn("avg_Wind_Chill",round(col("avg_Wind_Chill")))
  dataAvgWindChill.show()

  val dataTempMeanValue = myNewData
    .filter(col("Temperature").isNotNull)
    .groupBy("Severity")
    .avg("Temperature")
    .withColumnRenamed("avg(Temperature)","avg_Temperature")
    .withColumn("avg_Temperature",round(col("avg_Temperature")))
  dataTempMeanValue.show()

  val myFinalData = myNewData
    .join(dataAvgWindChill,"Severity")
    .withColumn("Wind_Chill",when(col("Wind_Chill").isNull,col("avg_Wind_Chill")).otherwise(col("Wind_Chill")))
    .drop("avg_Wind_Chill")
    .join(dataTempMeanValue,"Severity")
    .withColumn("Temperature",when(col("Temperature").isNull,col("avg_Temperature")).otherwise(col("Temperature")))
    .drop("avg_Temperature")

  myFinalData.show()

  //Nombre d'accident pour par séverité
  val dataAccPerSev = myFinalData
    .groupBy(col("Severity"))
    .agg(count("ID"))
    .withColumnRenamed("count(ID)","acc_num_per_sev")
    .withColumn("acc_rate_per_sev",round(col("acc_num_per_sev")/sum(col("acc_num_per_sev")).over(),4))
    .withColumn("acc_rate_per_sev",round(col("acc_rate_per_sev")*100,2))
  dataAccPerSev.show()

  //Nombre d'accidents pour chaque température

  val dataCountPerTemp = myFinalData
    .groupBy("Temperature")
    .agg(count("ID"))
    .withColumnRenamed("count(ID)","accident_num")
    .orderBy(desc("accident_num"))
  dataCountPerTemp.show()


  //Nombre d'accidents par gravité pour chaque wind_chill

  val dataCountPerWindChill = myFinalData
    .groupBy("Wind_Chill")
    .agg(count("ID"))
    .withColumnRenamed("count(ID)","accident_num")
    .orderBy(desc("accident_num"))
  dataCountPerWindChill.show()

  //Moyenne des températures par  sévérité

  val dataMeanTemPerSev = myFinalData
    .groupBy("Severity")
    .agg(avg("Temperature"))
    .withColumnRenamed("avg(Temperature)","Temp_Mean")
    .withColumn("Temp_Mean",round(col("Temp_Mean"),1))
    .orderBy("Severity")

  dataMeanTemPerSev.show()

  val dataMeanWindChillPerSev = myFinalData
    .groupBy("Severity")
    .agg(avg("Wind_Chill"))
    .withColumnRenamed("avg(Wind_Chill)","Wind_Chill_Mean")
    .withColumn("Wind_Chill_Mean",round(col("Wind_Chill_Mean"),1))
    .orderBy("Severity")

  dataMeanWindChillPerSev.show()

/*

  val dataWeatherCondition = myFinalData
    .groupBy("Weather_Condition")
    .agg(count("ID"))
    .withColumnRenamed("count(ID)", "acc_num_per_wc")
    .withColumn("acc_rate",round(col("acc_num_per_wc")/sum(col("acc_num_per_wc")).over(),4))
    .withColumn("acc_rate",round(col("acc_rate")*100,2))
    .orderBy(desc("acc_rate"))
  dataWeatherCondition.show()

  val dataWeatherConditionPerSev = myFinalData
    .groupBy("Severity","Weather_Condition")
    .agg(count("ID"))
    .withColumnRenamed("count(ID)", "acc_num_per_sev_per_wc")
    .join(dataAccPerSev,"Severity")
    .withColumn("acc_rate",round(col("acc_num_per_sev_per_wc")/col("acc_num_per_sev"),4))
    .withColumn("acc_rate",round(col("acc_rate")*100,2))
    .orderBy(desc("Severity"))
  dataWeatherConditionPerSev.show()

  val dataHotTemp = myFinalData
    .filter("Temperature>=25")
    .filter("Wind_Chill>=22")
    .groupBy(col("Severity"))
    .count()
    .withColumnRenamed("count","acc_num_hot_Temp")
    .join(dataAccPerSev,"Severity")
    .withColumn("hot_temp_acc_rate_per",round(col("acc_num_hot_Temp")/col("acc_num_per_sev"),4))
    .withColumn("hot_temp_acc_rate_per",round(col("hot_temp_acc_rate_per")*100,2))
    .orderBy(desc("Severity"))

  dataHotTemp.show()

  val dataColdTemp = myFinalData
    .filter("Temperature<=-1")
    .filter("Wind_Chill<=0")
    .groupBy(col("Severity"))
    .count()
    .withColumnRenamed("count","acc_num_cold_Temp")
    .join(dataAccPerSev,"Severity")
    .withColumn("cold_temp_acc_rate_per_sev",round(col("acc_num_cold_Temp")/col("acc_num_per_sev"),4))
    .withColumn("cold_temp_acc_rate_per_sev",round(col("cold_temp_acc_rate_per_sev")*100,2))
    .orderBy(desc("Severity"))

  dataColdTemp.show()

 val dataVeryHighHumidity = myFinalData
   .filter("Humidity>=70")
    .groupBy("Severity")
    .count()
    .withColumnRenamed("count","acc_num_high_humid")
   .join(dataAccPerSev,"Severity")
   .withColumn("high_humid_acc_rate_per_sev",round(col("acc_num_high_humid")/col("acc_num_per_sev"),4))
   .withColumn("high_humid_acc_rate_per_sev",round(col("high_humid_acc_rate_per_sev")*100,2))
   .orderBy(desc("Severity"))

  dataVeryHighHumidity.show()

  val dataVeryLowHumidity = myFinalData
    .filter("Humidity<=30")
    .groupBy("Severity")
    .count()
    .withColumnRenamed("count","acc_num_low_humid")
    .join(dataAccPerSev,"Severity")
    .withColumn("low_humid_acc_rate_per_sev",round(col("acc_num_low_humid")/col("acc_num_per_sev"),4))
    .withColumn("low_humid_acc_rate_per_sev",round(col("low_humid_acc_rate_per_sev")*100,2))
    .orderBy(desc("Severity"))

  dataVeryLowHumidity.show()

  val dataNormalHumidity = myFinalData
    .filter("Humidity<70 AND Humidity>30")
    .groupBy("Severity")
    .count()
    .withColumnRenamed("count","acc_num_normal_humid")
    .join(dataAccPerSev,"Severity")
    .withColumn("normal_humid_acc_rate_per_sev",round(col("acc_num_normal_humid")/col("acc_num_per_sev"),4))
    .withColumn("normal_humid_acc_rate_per_sev",round(col("normal_humid_acc_rate_per_sev")*100,2))
    .orderBy(desc("Severity"))

  dataNormalHumidity.show()

  val dataDayPeriod = myFinalData
    .groupBy("Sunrise_Sunset")
    .count()
    .withColumnRenamed("count","acc_num_per_day_period")
  dataDayPeriod.show()

  val dataDayPerSev = myFinalData
    .filter(col("Sunrise_Sunset").contains("Day"))
    .groupBy("Severity")
    .count()
    .withColumnRenamed("count","day_acc_num_per_sev")
    .join(dataAccPerSev,"Severity")
    .withColumn("day_acc_rate_per_sev",round(col("day_acc_num_per_sev")/col("acc_num_per_sev"),4))
    .withColumn("day_acc_rate_per_sev",round(col("day_acc_rate_per_sev")*100,2))
    .orderBy(desc("Severity"))
  dataDayPerSev.show()


  val dataNightPerSev = myFinalData
    .filter(col("Sunrise_Sunset").contains("Night"))
    .groupBy("Severity")
    .count()
    .withColumnRenamed("count","night_acc_num_per_sev")
    .join(dataAccPerSev,"Severity")
    .withColumn("night_acc_rate_per_sev",round(col("night_acc_num_per_sev")/col("acc_num_per_sev"),4))
    .withColumn("night_acc_rate_per_sev",round(col("night_acc_rate_per_sev")*100,2))
    .orderBy(desc("Severity"))
  dataNightPerSev.show()
*/

  spark.close()
}
