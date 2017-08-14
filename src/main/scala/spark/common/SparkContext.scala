package spark.common

import org.apache.spark.sql.SparkSession

object SparkContexts {
  //  val conf = new SparkConf()
  //  .setMaster("local[2]")
  //  .setAppName("PqtoTsv")
  //  val sc = new SparkContext(conf)
  //  val processTime = new java.sql.Timestamp(sc.startTime)
  //  val sqlContext = new SQLContext(sc)
  val spark = SparkSession
    .builder()
    //    .master("local[*]")
    //    .appName("Sphere Spark ETL")
    //    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val sc = spark.sparkContext
  val processTime = new java.sql.Timestamp(spark.sparkContext.startTime)
  val sqlContext = spark.sqlContext
}