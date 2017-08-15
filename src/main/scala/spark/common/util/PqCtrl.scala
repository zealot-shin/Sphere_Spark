package spark.common.util

import org.apache.spark.sql.{DataFrame, SaveMode}

class PqCtrl(val pqName: String, val saveMode: SaveMode) {

  def write(df: DataFrame): Unit = {
    df.write.mode(saveMode).parquet(pqName)
  }
}
