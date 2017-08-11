package testCompo

import app.common.base.SparkApp
import app.common.executor.Executor
import app.common.template.DbToPq
import org.apache.spark.sql.DataFrame
import org.bson.Document
import spark.common.util._
import org.apache.spark.sql.functions._


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
                     name: String
                   )

object TestCompo extends SparkApp {
  def exec(implicit args: Array[String]) = {
    compo.run()
  }

  val compo = new DbToPq with Executor {

    val sourceStation = new HBTV
    val readTableName = "nSite.wf.workitems"
    val readSchema = null
    val matchQuery = Document.parse("{ $match: { state : { $in : ['Terminated','Completed','Exception']} } } ")
    val projection: Document = null

    def invoke(df: DataFrame)(implicit args: Array[String]) = {
      df
        .withColumn("sourceSystem", lit("editsphere_v1"))
        .withColumn("sourceStation", lit("HBTV"))
        .withColumn("name", editName(col("name")))
        .show(5, false)
      println(df.count)
      df
    }
  }

  val editName = udf { (name: String) => {
    name match {
      case null => null
      case _ => name.replaceAll("\\||\r|\n", " ")
    }
  }
  }
}
