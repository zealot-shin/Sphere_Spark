package app.common.reader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.StructsToJson
import org.apache.spark.sql.types.StructType
import org.bson.Document
import spark.common.util._


trait DbReader {

  val sourceStation: SourceStation
  val readTableName: String
  val schema: StructType = null
  val matchQuery: Document = null
  val projection: Document = null
  val columns: Array[String] = null
  val readDbWhere: Array[String] = Array("1 = 1")
  val predicates: Array[String] = null


  def readDb(): DataFrame = {
    val dbCtrl = new DbCtrl(sourceStation, readTableName, schema, matchQuery, projection, columns, readDbWhere, predicates)
    dbCtrl.read()
  }

}