package app.common.base

import org.joda.time.DateTime

trait SparkApp {

  val DATE_FORMAT = "yy/MM/dd HH:mm:ss"

  def exec(implicit args: InArgs): Unit

  def runner(args: Array[String]) {
    println(s"${new DateTime toString (DATE_FORMAT)} INFO START")
    if (args(3) == "true") {
      args.foreach(println)
    }
    args(0).split(",").foreach { appId =>
      try exec(InArgs(appId, args(1), args(2), args(3))) catch {
        case e: Throwable => println(s"${new DateTime toString (DATE_FORMAT)} ERROR ${e.toString()}"); throw e
      }
    }
    println(s"${new DateTime toString (DATE_FORMAT)} INFO FINISHED")
  }

  def main(args: Array[String]) = {
    runner(args)
  }
}
