package app.common.reader

import org.apache.spark.sql.DataFrame
import spark.common.util._
import org.bson.Document


trait DbReader {

  val sourceStation: SourceStation
  val readTableName: String
  val readSchema: ReadSchema = null
  val matchQuery: Document = null
  val projection: Document = null
  val columns: Array[String] = null
  val readDbWhere: Array[String] = Array("1 = 1")
  val predicates: Array[String] = null


  def readDb(): DataFrame = {
    val dbCtrl = new DbCtrl(sourceStation, readTableName, readSchema, matchQuery, projection, columns,readDbWhere, predicates)
    dbCtrl.read()
  }

}