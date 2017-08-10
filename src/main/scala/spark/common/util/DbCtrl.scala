package spark.common.util

import app.common.reader.MongoReader
import org.apache.spark.sql.DataFrame
import app.common.reader.DbReader

trait SourceStation
case class EditSphere() extends SourceStation
case class NewSphere() extends SourceStation
case class HBTV() extends SourceStation
case class YNTV() extends SourceStation
case class JSTV() extends SourceStation
case class SMG() extends SourceStation

class DbCtrl(val sourceStation : SourceStation ,val readDatabase:String,val readTableName:String ) {

  val ipAddress = sourceStation match {
    case EditSphere() => "114.115.147.192:30000"
    case  NewSphere() => "114.115.151.26"
    case  HBTV() | YNTV() | JSTV() =>"10.6.5.249"
    case SMG() => " 10.6.5.248"
  }
  def read(): DataFrame = {
    MongoReader(ipAddress,readDatabase,readTableName).read()
  }
}
