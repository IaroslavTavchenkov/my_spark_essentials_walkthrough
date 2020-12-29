import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object DataFramesBasics extends App {

  // start a session
  val sparkSession = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  /*
  * read our DataFrame
  * don't use "inferSchema" in production (see 14 minute in 5th episode)
  * */
  val firstDF = sparkSession
    .read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  firstDF.show()
  firstDF.printSchema()

  firstDF.take(10).foreach(println)

  // spark type
  val longType = LongType

  val carSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", IntegerType),
      StructField("Cylinders", IntegerType),
      StructField("Displacement", IntegerType),
      StructField("Horsepower", IntegerType),
      StructField("Weight_in_lbs", IntegerType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    )
  )

  //  obtain schema
  val carsDFSchema = firstDF.schema
  println(s"here is my carsDFSchema: $carsDFSchema")

  //   reading DataFrame with my own schema
  val carDFWithSchema = sparkSession
    .read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")
  carDFWithSchema.take(5).foreach(println)

  // creating rows by hand
  val myRow = Row("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA")
  println(s"here is myRow: $myRow")

  // creating rows with tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )

  //   schema auto-inferred
  val manualCarsDF = sparkSession.createDataFrame(cars)
  println(s"here is my manualCarsDF: $manualCarsDF")

  //   DF with implicits
  import sparkSession.implicits._
  val manualCarsDFWithImplicits = cars.toDF(
    "Name",
    "MPG",
    "Cylinders",
    "Displacement",
    "HP",
    "Weight_in_lbs",
    "Acceleration",
    "Year",
    "CountryOrigin")

  // compare output
  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()
}
