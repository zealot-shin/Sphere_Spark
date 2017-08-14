package app.common.template.base

import app.common.base.InArgs
import app.common.flow.OneInToOneOut
import app.common.reader.DbReader
import org.apache.spark.sql.DataFrame

trait DbToAny extends OneInToOneOut[Unit, Unit] with DbReader {
  def preExec(in: Unit)(implicit args: InArgs): DataFrame = readDb()
}
