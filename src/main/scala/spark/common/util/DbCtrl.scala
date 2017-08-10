package spark.common.util

import app.common.reader.MongoReader
import org.apache.spark.sql.DataFrame

trait SourceStation
class HBTV extends SourceStation
class YNTV extends SourceStation
class JSTV extends SourceStation
class SMG extends SourceStation
class EDITSPHERE extends SourceStation
class NEWSPHERE extends SourceStation


class DbCtrl {

  def read(): DataFrame = {
    MongoReader("").read()
  }
}
