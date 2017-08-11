package app.common.template.base

import app.common.flow.OneInToOneOut
import app.common.writer.PqWriter
import org.apache.spark.sql.DataFrame

trait AnyToPq extends OneInToOneOut[Unit, Unit] with PqWriter {
  def postExec(df: DataFrame)(implicit args: Array[String]): Unit = writePq(df)
}
