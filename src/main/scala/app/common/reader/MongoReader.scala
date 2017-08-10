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


def apply(ip:String,readDatabase:String,readTableName:String) = {

  val readConfig = ReadConfig(Map("uri" -> s"mongodb://${ip}/${readDatabase}.${readTableName}?readPreference=primaryPreferred"))

  new MongoReader(readConfig)
}
}

class MongoReader(rc:ReadConfig) {
  def read() : DataFrame =  {
    val rdd = MongoSpark.load(sc ,rc).withPipeline(Seq( Document.parse("{ $match: { state : { $in : ['Terminated','Completed','Exception']} } } ")))
    rdd.toDF[Workitem]
  }
}
