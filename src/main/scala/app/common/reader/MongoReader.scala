package app.common.reader

import com.mongodb.spark._
import org.bson.Document
import com.mongodb.spark.config._
import org.apache.spark.sql.DataFrame
import spark.common.SparkContexts._

case class Workitem(
                     _id: String,
                     _type: String,
                     activityDefineName: String,
                     activityTemplateName: String,
                     worker: String,
                     state: String,
                     createdTime: String,
                     createdBy: String,
                     startTime: String,
                     stopTime: String,
                     name: String,
                     sourceSystem: String = "editsphere_v1",
                     sourceStation: String = "CDV"
                   )

object MongoReader {

  val readConfig = ReadConfig(Map("uri" -> "mongodb://114.115.147.192:30000/nsitedb.nSite.wf.workitems?readPreference=primaryPreferred"))

 def read() : DataFrame =  {
   val rdd = MongoSpark.load(sc ,readConfig).withPipeline(Seq( Document.parse("{ $match: { state : { $in : ['Terminated','Completed','Exception']} } } ")))
   rdd.toDF[Workitem]
  }
}
