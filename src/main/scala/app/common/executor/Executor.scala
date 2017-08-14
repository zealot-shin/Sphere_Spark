package app.common.executor

import app.common.base.InArgs
import org.apache.spark.sql.DataFrame

trait Executor {
  def invoke(df: DataFrame)(implicit args: InArgs): DataFrame
}
