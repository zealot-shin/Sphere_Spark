package app.common.writer

import org.apache.spark.sql.{DataFrame, SaveMode}
import spark.common.util.PqCtrl

trait PqWriter {

  val writerPqName: String
  val writeMode: SaveMode

  def writePq(df: DataFrame): Unit = {
    val pqCtrl = new PqCtrl(writerPqName, writeMode)
    pqCtrl.write(df)
  }
}
