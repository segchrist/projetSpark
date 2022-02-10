import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, round, sum}

object geographical extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark Projet")
    .master("local[*]")
    .getOrCreate()

  val accident_data = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("US_Accidents_Dec20_updated.csv")

  val states_data = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("states.csv")

  val my_data= accident_data.join(states_data, states_data("Abbreviation") === accident_data("State"), "inner" )

  //show 10 rows of our DataFrame
  my_data.show(10)

  //count number of rows of our dataFrame
  val num_rows = my_data.count()
  println("number of rows: ", num_rows)
  //show our dataFrame schema
  //newData.printSchema()

  //show statistic of the data we want


  // - La situation géographique (state, city, county);

  //List of states
  val all_states =  my_data.select(col("State"),col("StateName")).distinct()

  println("Number of states: ", my_data("State"))

  //List of states
  all_states.show()

  //Top 10 of states have many accidents
  println("Top 10 of states have many accidents")
  val nb_accidents_by_state = my_data.groupBy("State", "StateName")
    .count()
    .orderBy(desc("count"))
    .withColumn("Percent", round(col("count") / sum("count").over(), 3))
  nb_accidents_by_state.show(10)

  //List of cities and county by city
  my_data.groupBy("State", "City").count().orderBy("State").show(100)


  //Top 10 of city have many accidents
  println("Top 10 of city have many accidents")
  val nb_accidents_by_city = my_data.groupBy("State", "StateName", "City")
    .count()
    .orderBy(desc("count"))
    .withColumn("Percent", round(col("count") / sum("count").over(), 3))
  nb_accidents_by_city.show(10)

  println("Top 10 of county have many accidents")
  //Top 10 of county have many accidents
  val nb_accidents_by_county= my_data.groupBy("State", "StateName", "County")
    .count()
    .orderBy(desc("count"))
    .withColumn("Percent",round(col("count") / sum("count").over(), 3))
  nb_accidents_by_county.show(10)

  //println(newData.select("Severity").groupBy("Severity").count().orderBy("count").show())

  //val acc_by_city = newData.groupBy("City").count().orderBy().show()

  println("Severity = 4")
  println("Severity  by state")
  val nb_accidents_by_state_severity_4 = my_data.filter("Severity==4").groupBy("State", "StateName")
    .count()
    //.orderBy(desc("count"))
    .withColumn("Percent", round(col("count") / sum("count").over(), 3))
  nb_accidents_by_state_severity_4.show(20)

  println("Severity = 3")
  val nb_accidents_by_state_severity_3= my_data.filter("Severity==3").groupBy("State", "StateName")
    .count()
    //.orderBy(desc("count"))
    .withColumn("Percent", round(col("count") / sum("count").over(), 3))
  nb_accidents_by_state_severity_3.show(20)

  println("Severity = 2")
  val nb_accidents_by_state_severity_2= my_data.filter("Severity==2").groupBy("State", "StateName")
    .count()
    //.orderBy(desc("count"))
    .withColumn("Percent", round(col("count") / sum("count").over(), 3))
  nb_accidents_by_state_severity_2.show(20)

  println("Severity = 1")
  val nb_accidents_by_state_severity_1= my_data.filter("Severity==1").groupBy("State", "StateName")
    .count()
    //.orderBy(desc("count"))
    .withColumn("Percent", round(col("count") / sum("count").over(), 3))

    nb_accidents_by_state_severity_1.show(20)
}
