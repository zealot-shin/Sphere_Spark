package app.common.reader

import com.mongodb.spark._
import org.bson.Document
import com.mongodb.spark.config._
import org.apache.spark.sql.DataFrame
import spark.common.SparkContexts._
import testCompo.Workitem

trait ReadSchema

object MongoReader {


  def apply(ipAndDb: String, readTableName: String, rs: ReadSchema, matchQuery: Document, projection: Document) = {

    val readConfig = ReadConfig(Map("uri" -> s"${ipAndDb}.${readTableName}?readPreference=primaryPreferred"))
    new MongoReader(readConfig, rs, matchQuery, projection)
  }
}

class MongoReader(rc: ReadConfig, rs: ReadSchema, matchQuery: Document, projection: Document) {

  def read(): DataFrame = {

    (matchQuery, projection) match {
      case (mq, null) if !mq.isEmpty => {
        val rdd = MongoSpark.load(sc, rc).withPipeline(Seq(mq))
        rdd.toDF[Workitem]()
      }
      case (null, p) if !p.isEmpty => {
        val rdd = MongoSpark.load(sc, rc).withPipeline(Seq(p))
        rdd.toDF[Workitem]()
      }
      case _ => {
        val rdd = MongoSpark.load(sc, rc)
        rdd.toDF[Workitem]()
      }
    }
  }
}
