package app.common.reader

import org.apache.spark.sql.DataFrame
import spark.common.util._


trait DbReader {

  val sourceStation: SourceStation
  val readDatabase: String
  val readTableName: String

  def readDb(): DataFrame = {
    val dbCtrl = new DbCtrl(sourceStation, readDatabase, readTableName)
    dbCtrl.read()
  }



}