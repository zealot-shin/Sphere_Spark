package spark.common.util

import app.common.reader.MongoReader
import org.apache.spark.sql.DataFrame

class DbCtrl {

  def read(): DataFrame = {
    MongoReader.read()
  }
}
