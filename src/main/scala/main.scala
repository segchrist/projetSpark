import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc, round, sum}

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

  // myData.show()

  val routeData = myData
    .groupBy(
      col("Severity"),
      col("Amenity"),
      col("Bump"),
      col("Crossing"),
      col("Give_Way"),
      col("Junction"),
      col("No_Exit"),
      col("Railway"),
      col("Roundabout"),
      col("Station"),
      col("Stop"),
      col("Traffic_Signal")
    )
    .count()
    .withColumnRenamed("count", "nombre accident")
    .sort(desc("nombre accident"))

  val routeResume = routeData
    .groupBy(
      col("Amenity"),
      col("Bump"),
      col("Crossing"),
      col("Give_Way"),
      col("Junction"),
      col("No_Exit"),
      col("Railway"),
      col("Roundabout"),
      col("Station"),
      col("Stop"),
      col("Traffic_Signal")
    )
    .sum("nombre accident")
    .withColumnRenamed("sum(nombre accident)", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .sort(desc("nombre accident"))
 val severityResume = routeData
    .groupBy("Severity")
    .sum("nombre accident")
    .withColumnRenamed("sum(nombre accident)", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .orderBy(desc("Severity"))
  val amenityRoute = routeData
    .filter("Amenity == true")
    .groupBy(
      col("Severity"),
      col("Amenity"),
    )
    .sum("nombre accident")
    .withColumnRenamed("sum(nombre accident)", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .orderBy(desc("Severity"))
    .join(severityResume
      .withColumnRenamed("Percent","Global Percent")
      .withColumnRenamed("nombre accident", "Global nombre accident"),
      "Severity")
    .withColumn("Difference between global percent",
      round(col("Global Percent") - col("Percent"), 3))
    .select(
      col("Severity"),
      col("Amenity"),
      col("nombre accident"),
      col("Percent"),
      col("Difference between global percent")
    )
  val bumpRoute = routeData
    .filter("Bump == true")
    .groupBy(
      col("Severity"),
      col("Bump")
    )
    .sum("nombre accident")
    .withColumnRenamed("sum(nombre accident)", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .orderBy(desc("Severity"))
    .join(severityResume
      .withColumnRenamed("Percent","Global Percent")
      .withColumnRenamed("nombre accident", "Global nombre accident"),
      "Severity")
    .withColumn("Difference between global percent",
      round(col("Global Percent") - col("Percent"), 3))
    .select(
      col("Severity"),
      col("Bump"),
      col("nombre accident"),
      col("Percent"),
      col("Difference between global percent")
    )
  val crossingRoute = routeData
    .filter("Crossing == true")
    .groupBy(
      col("Severity"),
      col("Crossing")
    )
    .sum("nombre accident")
    .withColumnRenamed("sum(nombre accident)", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .orderBy(desc("Severity"))
    .join(severityResume
      .withColumnRenamed("Percent","Global Percent")
      .withColumnRenamed("nombre accident", "Global nombre accident"),
      "Severity")
    .withColumn("Difference between global percent",
      round(col("Global Percent") - col("Percent"), 3))
    .select(
      col("Severity"),
      col("Crossing"),
      col("nombre accident"),
      col("Percent"),
      col("Difference between global percent")
    )
  val giveWayRoute = routeData
    .filter("Give_Way == true")
    .groupBy(
      col("Severity"),
      col("Give_Way")
    )
    .sum("nombre accident")
    .withColumnRenamed("sum(nombre accident)", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .orderBy(desc("Severity"))
    .join(severityResume
      .withColumnRenamed("Percent","Global Percent")
      .withColumnRenamed("nombre accident", "Global nombre accident"),
      "Severity")
    .withColumn("Difference between global percent",
      round(col("Global Percent") - col("Percent"), 3))
    .select(
      col("Severity"),
      col("Give_Way"),
      col("nombre accident"),
      col("Percent"),
      col("Difference between global percent")
    )
  val junctionRoute = routeData
    .filter("Junction == true")
    .groupBy(
      col("Severity"),
      col("Junction")
    )
    .sum("nombre accident")
    .withColumnRenamed("sum(nombre accident)", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .orderBy(desc("Severity"))
    .join(severityResume
      .withColumnRenamed("Percent","Global Percent")
      .withColumnRenamed("nombre accident", "Global nombre accident"),
      "Severity")
    .withColumn("Difference between global percent",
      round(col("Global Percent") - col("Percent"), 3))
    .select(
      col("Severity"),
      col("Junction"),
      col("nombre accident"),
      col("Percent"),
      col("Difference between global percent")
    )
  val noExitRoute = routeData
    .filter("No_Exit == true")
    .groupBy(
      col("Severity"),
      col("No_Exit")
    )
    .sum("nombre accident")
    .withColumnRenamed("sum(nombre accident)", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .orderBy(desc("Severity"))
    .join(severityResume
      .withColumnRenamed("Percent","Global Percent")
      .withColumnRenamed("nombre accident", "Global nombre accident"),
      "Severity")
    .withColumn("Difference between global percent",
      round(col("Global Percent") - col("Percent"), 3))
    .select(
      col("Severity"),
      col("No_Exit"),
      col("nombre accident"),
      col("Percent"),
      col("Difference between global percent")
    )
  val railwayRoute = routeData
    .filter("Railway == true")
    .groupBy(
      col("Severity"),
      col("Railway")
    )
    .sum("nombre accident")
    .withColumnRenamed("sum(nombre accident)", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .orderBy(desc("Severity"))
    .join(severityResume
      .withColumnRenamed("Percent","Global Percent")
      .withColumnRenamed("nombre accident", "Global nombre accident"),
      "Severity")
    .withColumn("Difference between global percent",
      round(col("Global Percent") - col("Percent"), 3))
    .select(
      col("Severity"),
      col("Railway"),
      col("nombre accident"),
      col("Percent"),
      col("Difference between global percent")
    )
  val roundaboutRoute = routeData
    .filter("Roundabout == true")
    .groupBy(
      col("Severity"),
      col("Roundabout")
    )
    .sum("nombre accident")
    .withColumnRenamed("sum(nombre accident)", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .orderBy(desc("Severity"))
    .join(severityResume
      .withColumnRenamed("Percent","Global Percent")
      .withColumnRenamed("nombre accident", "Global nombre accident"),
      "Severity")
    .withColumn("Difference between global percent",
      round(col("Global Percent") - col("Percent"), 3))
    .select(
      col("Severity"),
      col("Roundabout"),
      col("nombre accident"),
      col("Percent"),
      col("Difference between global percent")
    )
  val stationRoute = routeData
    .filter("Station == true")
    .groupBy(
      col("Severity"),
      col("Station")
    )
    .sum("nombre accident")
    .withColumnRenamed("sum(nombre accident)", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .orderBy(desc("Severity"))
    .join(severityResume
      .withColumnRenamed("Percent","Global Percent")
      .withColumnRenamed("nombre accident", "Global nombre accident"),
      "Severity")
    .withColumn("Difference between global percent",
      round(col("Global Percent") - col("Percent"), 3))
    .select(
      col("Severity"),
      col("Station"),
      col("nombre accident"),
      col("Percent"),
      col("Difference between global percent")
    )
  val stopRoute = routeData
    .filter("Stop == true")
    .groupBy(
      col("Severity"),
      col("Stop")
    )
    .sum("nombre accident")
    .withColumnRenamed("sum(nombre accident)", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .orderBy(desc("Severity"))
    .join(severityResume
      .withColumnRenamed("Percent","Global Percent")
      .withColumnRenamed("nombre accident", "Global nombre accident"),
      "Severity")
    .withColumn("Difference between global percent",
      round(col("Global Percent") - col("Percent"), 3))
    .select(
      col("Severity"),
      col("Stop"),
      col("nombre accident"),
      col("Percent"),
      col("Difference between global percent")
    )
  val trafficSignalRoute = routeData
    .filter("Traffic_Signal == true")
    .groupBy(
      col("Severity"),
      col("Traffic_Signal")
    )
    .sum("nombre accident")
    .withColumnRenamed("sum(nombre accident)", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .orderBy(desc("Severity"))
    .join(severityResume
      .withColumnRenamed("Percent","Global Percent")
      .withColumnRenamed("nombre accident", "Global nombre accident"),
      "Severity")
    .withColumn("Difference between global percent",
      round(col("Global Percent") - col("Percent"), 3))
    .select(
      col("Severity"),
      col("Traffic_Signal"),
      col("nombre accident"),
      col("Percent"),
      col("Difference between global percent")
    )

  //routeResume.show(5)
  //severityResume.show()
  /*amenityRoute.show()
  bumpRoute.show()*/
  /*crossingRoute.show()
  giveWayRoute.show()*/
  /*junctionRoute.show()
  noExitRoute.show()*/
  /*railwayRoute.show()
  roundaboutRoute.show()*/
  /*stationRoute.show()
  stopRoute.show()*/
  trafficSignalRoute.show()

  spark.close()
}
