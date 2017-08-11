package app.common.writer

import org.apache.spark.sql.{DataFrame, SaveMode}
import spark.common.util.PqCtrl

trait PqWriter {

  val writerPqPath: String
  val writeMode: SaveMode

  def writePq(df: DataFrame): Unit = {
    val pqCtrl = new PqCtrl(writerPqPath, writeMode)
    pqCtrl.write(df)
  }
}
