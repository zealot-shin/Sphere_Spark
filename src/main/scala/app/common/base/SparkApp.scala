package app.common.base

import org.joda.time.DateTime

trait SparkApp {

  val DATE_FORMAT = "yy/MM/dd HH:mm:ss"

  def exec(implicit args: Array[String]): Unit

  def runner(args: Array[String]) {
    println(s"${new DateTime toString (DATE_FORMAT)} INFO START")
    if (args(3) == "true") {
      args.foreach(println)
    }
    try exec(args: Array[String]) catch {
      case e: Throwable => println(s"${new DateTime toString (DATE_FORMAT)} ERROR ${e.toString()}"); throw e
    }
    println(s"${new DateTime toString (DATE_FORMAT)} INFO FINISHED")
  }

  def main(args: Array[String]) = {
    runner(args)
  }
}
