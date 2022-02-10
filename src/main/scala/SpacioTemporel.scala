import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, date_format, desc, round, sum, to_timestamp, year}

object SpacioTemporel extends App{
  val spark = SparkSession
    .builder()
    .appName("Spark Projet")
    .master("local[*]")
    .getOrCreate()
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val myData = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("US_Accidents_Dec20_updated.csv")

  val a = myData.select(
    col("ID"),
    col("Severity"),
    col("Start_Time"),
    col("End_Time")
  )
    .withColumn("Timestamp Start_Time",
      to_timestamp(col("Start_Time"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("Timestamp End_Time",
      to_timestamp(col("End_Time"), "yyyy-MM-dd HH:mm:ss"))

  val b = a
    .withColumn("Year", year(col("Timestamp Start_Time")))
    .withColumn("Diff seconde",
      col("Timestamp End_Time").cast("Integer") - col("Timestamp Start_Time").cast("Integer"))
    .withColumn("Diff minute",
      (col("Timestamp End_Time").cast("Integer") - col("Timestamp Start_Time").cast("Integer")) / 60)
    .withColumn("Day", date_format(col("Timestamp Start_Time"), "yyyy-MM-dd"))

  val c = b
    .groupBy(
      col("Year")
    )
    .count()
    .withColumnRenamed("count", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .sort(desc("Year"))

  val d = b
    .groupBy(
      col("Year"),
      col("Severity")
    ).count()
    .withColumnRenamed("count", "nombre accident")
    .withColumn("Percent",
      round(col("nombre accident") / sum("nombre accident").over(), 3))
    .sort(desc("nombre accident"))

  val e = b
    .groupBy(
      col("Severity")
    ).avg("Diff minute")
    .withColumnRenamed("avg(Diff minute)", "Average Duration (minute)")
    .sort(desc("Severity"))

  val f = b
    .groupBy("Day", "Severity")
    .count()
    .withColumnRenamed("count", "nombre accident")
    .groupBy("Severity")
    .avg("nombre accident")
    .withColumnRenamed("avg(nombre accident)", "nombre accident moyenne par severité")
    .orderBy("Severity")

  b.show()
  c.show()
  d.show()
  e.show()
  f.show()
  spark.close()
}
