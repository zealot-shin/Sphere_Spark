package app.common.flow

import spark.common.Logging
import org.apache.spark.sql.DataFrame

trait OneInToOneOut[IN, OUT] extends Logging {
  def preExec(in: IN)(implicit args: Array[String]): DataFrame
  def exec(df: DataFrame)(implicit args: Array[String]): DataFrame
  def postExec(df: DataFrame)(implicit args: Array[String]): OUT

  final def run(in: IN)(implicit args: Array[String]): OUT = {
    val input = try {
      preExec(in)
    } catch {
      case t: Throwable => platformError(t); throw t
    }
    if (args(3) == "true") {
      println(s"${args(0)}{input}")
      input.show(false)
    }

    val output = try {
      exec(input)
    } catch {
      case t: Throwable => appError(t); throw t
    }

    if (args(3) == "true") {
      println(s"${args(0)}{output}")
      output.show(false)
    }

    try {
      postExec(output)
    } catch {
      case t: Throwable => platformError(t); throw t
    }
  }
  final def debug(in: IN)(implicit args: Array[String]): OUT =
    run(in)(Array(args(0), args(1), args(2), "true"))
}