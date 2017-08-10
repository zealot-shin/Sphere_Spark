package app.common.template.base

import app.common.flow.OneInToOneOut
import org.apache.spark.sql.DataFrame
import app.common.reader.DbReader.readDb

trait DbToAny extends OneInToOneOut[Unit,Unit]{
  def preExec(in: Unit)(implicit args: Array[String]): DataFrame = readDb()
}
