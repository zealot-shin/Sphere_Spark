package app.common.reader

import org.apache.spark.sql.DataFrame
import spark.common.util._
import org.bson.Document


trait DbReader {

  val sourceStation: SourceStation
  val readTableName: String
  val readSchema: ReadSchema

   val matchQuery: Document
   val projection: Document

  def readDb(): DataFrame = {
    val dbCtrl = new DbCtrl(sourceStation, readTableName, readSchema, matchQuery, projection)
    dbCtrl.read()
  }


}