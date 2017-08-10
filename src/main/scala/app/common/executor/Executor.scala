package app.common.executor

import org.apache.spark.sql.DataFrame

trait Executor {
 def invoke(df : DataFrame)(implicit args: Array[String]) : DataFrame
}
