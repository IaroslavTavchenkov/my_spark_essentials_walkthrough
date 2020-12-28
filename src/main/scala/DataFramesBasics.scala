import org.apache.spark.sql.SparkSession

object DataFramesBasics extends App {

  // start a session
  val sparkSession = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // read our DataFrame
  val firstDF = sparkSession
    .read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  firstDF.show()
}
