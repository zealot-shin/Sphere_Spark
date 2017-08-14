package app.common.template

import app.common.base.InArgs
import app.common.executor.Executor
import app.common.template.base.{AnyToPq, DbToAny}
import org.apache.spark.sql.DataFrame

trait DbToPq extends DbToAny with AnyToPq with Executor {
  def exec(df: DataFrame)(implicit args: InArgs): DataFrame = invoke(df)
}
