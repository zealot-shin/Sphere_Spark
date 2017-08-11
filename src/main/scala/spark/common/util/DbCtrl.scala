package spark.common.util

import app.common.reader.{DbReader, MongoReader, ReadSchema}
import org.apache.spark.sql.DataFrame
import org.bson.Document

trait SourceStation

case class EditSphere() extends SourceStation

case class NewSphere() extends SourceStation

case class HBTV() extends SourceStation

case class YNTV() extends SourceStation

case class JSTV() extends SourceStation

case class SMG() extends SourceStation

class DbCtrl(val sourceStation: SourceStation, val readTableName: String, val readSchema: ReadSchema, val matchQuery: Document, val projection: Document) {

  val ipAndDb = sourceStation match {
    case EditSphere() => "mongodb://114.115.147.192:30000/nsitedb"
    case NewSphere() => "114.115.151.26"
    case HBTV() => "mongodb://10.6.5.249/nsitedb_hbtv"
    case YNTV() => "mongodb://10.6.5.249/nsitedb_yntv"
    case JSTV() => "mongodb://10.6.5.249/nsitedb_jstv"
    case SMG() => " 10.6.5.248"
  }

  def read(): DataFrame = {
    MongoReader(ipAndDb, readTableName, readSchema, matchQuery, projection).read()
  }
}
