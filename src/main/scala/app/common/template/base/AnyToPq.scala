package app.common.template.base

import app.common.flow.OneInToOneOut
import org.apache.spark.sql.DataFrame

trait AnyToPq extends OneInToOneOut[Unit, Unit] {
  def postExec(df: DataFrame)(implicit args: Array[String]): Unit = {
//    df.write.parquet("/data/test/testpq")
  }
}
