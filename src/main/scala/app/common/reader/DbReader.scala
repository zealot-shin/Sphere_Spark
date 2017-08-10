package app.common.reader

import org.apache.spark.sql.DataFrame
import spark.common.util._


object DbReader {
  def readDb(): DataFrame = {
    val dbCtrl = new DbCtrl
    dbCtrl.read()
  }
}
