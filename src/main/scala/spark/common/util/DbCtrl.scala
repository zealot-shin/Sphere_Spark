package spark.common.util

import java.io.FileNotFoundException
import java.util.Properties

import app.common.reader.{MongoReader, MysqlReader}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.bson.Document

trait SourceStation

case class EditSphere() extends SourceStation

case class NewSphere() extends SourceStation

case class HBTV() extends SourceStation

case class YNTV() extends SourceStation

case class JSTV() extends SourceStation

case class SMG() extends SourceStation

class DbCtrl(val sourceStation: SourceStation, val readTableName: String, val schema: StructType, val matchQuery: Document,
             val projection: Document, val columns: Array[String], val readDbWhere: Array[String], val predicates: Array[String]) {

  val (ipAndDb, dbType) = sourceStation match {
    case EditSphere() => ("mongodb://114.115.147.192:30000/nsitedb", "mongo")
    case NewSphere() => ("jdbc:mysql://114.115.151.26/novadb", "mysql")
    case HBTV() => ("mongodb://10.6.5.249/nsitedb_hbtv", "mongo")
    case YNTV() => ("mongodb://10.6.5.249/nsitedb_yntv", "mongo")
    case JSTV() => ("mongodb://10.6.5.249/nsitedb_jstv", "mongo")
    case SMG() => ("jdbc:mysql://10.6.5.248/novadb", "mysql")
  }

  val prop: Properties = new Properties()

  def read(): DataFrame = {
    dbType match {
      case "mongo" => MongoReader(ipAndDb, readTableName, schema, matchQuery, projection).read()
      case "mysql" => {
        getProp()
        MysqlReader(prop, ipAndDb, readTableName, columns, readDbWhere, predicates).read()
      }
    }
  }

  def getProp() {
    val propFileName = sourceStation match {
      case NewSphere() => "newsphereDbInfo.properties"
      case _ => "smgDbInfo.properties"
    }

    try {
      //      val propFileIS = new FileInputStream(propFileName)
      val propFileIS = getClass.getClassLoader.getResourceAsStream(propFileName)
      prop.load(propFileIS)
      propFileIS.close()
    } catch {
      case e: FileNotFoundException => e.printStackTrace(); println("读取db配置文件失败! 原因：文件路径错误或者文件不存在")
    }
  }
}
