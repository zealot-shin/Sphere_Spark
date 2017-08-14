package app.common.flow

import app.common.base.InArgs
import org.apache.spark.sql.DataFrame
import spark.common.Logging

trait OneInToOneOut[IN, OUT] extends Logging {
  def preExec(in: IN)(implicit args: InArgs): DataFrame

  def exec(df: DataFrame)(implicit args: InArgs): DataFrame

  def postExec(df: DataFrame)(implicit args: InArgs): OUT

  final def run(in: IN)(implicit args: InArgs): OUT = {
    val input = try {
      preExec(in)
    } catch {
      case t: Throwable => platformError(t); throw t
    }
    if (args.deBug == "true") {
      println(s"${args.appId}{input}")
      input.show(false)
    }

    val output = try {
      exec(input)
    } catch {
      case t: Throwable => appError(t); throw t
    }

    if (args.deBug == "true") {
      println(s"${args.appId}{output}")
      output.show(false)
    }

    try {
      postExec(output)
    } catch {
      case t: Throwable => platformError(t); throw t
    }
  }

  final def debug(in: IN)(implicit args: InArgs): OUT =
    run(in)(args)
}