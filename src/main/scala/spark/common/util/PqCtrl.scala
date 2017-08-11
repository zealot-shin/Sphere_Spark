package spark.common.util

import org.apache.spark.sql.{DataFrame, SaveMode}
import spark.common.SparkContexts._

class PqCtrl(val pqPath: String,val saveMode:SaveMode) {

  def write(df: DataFrame): Unit = {

    df.write.mode(saveMode).parquet(pqPath)
  }
}
