package app.common.reader

import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.sql.DataFrame
import org.bson.Document
import spark.common.SparkContexts._
import org.apache.spark.sql.types.StructType

object MongoReader {


  def apply(ipAndDb: String, readTableName: String, schema: StructType, matchQuery: Document, projection: Document) = {

    val readConfig = ReadConfig(Map("uri" -> s"${ipAndDb}.${readTableName}?readPreference=primaryPreferred"))
    new MongoReader(readConfig, schema, matchQuery, projection)
  }
}

class MongoReader(rc: ReadConfig, schema: StructType, matchQuery: Document, projection: Document) {

  def read(): DataFrame = {

    (matchQuery, projection) match {
      case (mq, null) if !mq.isEmpty => {
        val rdd = MongoSpark.load(sc, rc).withPipeline(Seq(mq))
        rdd.toDF(schema)
      }
      case (null, p) if !p.isEmpty => {
        val rdd = MongoSpark.load(sc, rc).withPipeline(Seq(p))
        rdd.toDF(schema)
      }
      case _ => {
        val rdd = MongoSpark.load(sc, rc)
        rdd.toDF(schema)
      }
    }
  }
}
