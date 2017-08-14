package spark.common.util

import org.apache.spark.sql.{DataFrame, SaveMode}

class PqCtrl(val pqName: String, val saveMode: SaveMode) {

  def write(df: DataFrame): Unit = {
    // 对于集群环境，paPath应该是hdfs上的路径，对于本地环境可以选取本地的绝对路径。
    val pqPath = System.getenv("PqPath")

    df.write.mode(saveMode).parquet(pqPath + pqName)
  }
}
