package app.common.template.base

import app.common.flow.OneInToOneOut
import org.apache.spark.sql.DataFrame
import app.common.reader.DbReader

trait DbToAny extends OneInToOneOut[Unit,Unit]  with DbReader{
  def preExec(in: Unit)(implicit args: Array[String]): DataFrame = readDb()
}
