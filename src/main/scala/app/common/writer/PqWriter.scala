package app.common.writer

import app.common.base.InArgs
import app.common.consts.Const
import org.apache.spark.sql.{DataFrame, SaveMode}
import spark.common.util.PqCtrl

trait PqWriter {

  val writerPqName: String
  val writeMode: SaveMode

  def writePq(df: DataFrame)(implicit args: InArgs) {
    val pqName = Option(args.pqPath).getOrElse(Const.pqPath) + writerPqName
    val pqCtrl = new PqCtrl(pqName, writeMode)
    pqCtrl.write(df)
  }
}
