package testCompo

import app.common.base.SparkApp
import app.common.executor.Executor
import app.common.template.DbToPq
import org.apache.spark.sql.DataFrame
import org.bson.Document
import spark.common.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode._

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
    compo1.run()
    compo2.run()
  }

  val compo1 = new DbToPq with Executor {

    val compoName = ""
    val writerPqPath: String = "E:\\workspace\\SphereSpark\\data\\test/wf_item_hbtv"
    val writeMode = Overwrite
    val sourceStation = new HBTV
    val readTableName = "nSite.wf.workitems"
    override val matchQuery = Document.parse("{ $match: { state : { $in : ['Terminated','Completed','Exception']} } } ")

    def invoke(df: DataFrame)(implicit args: Array[String]) = {
      val pdf = df
        .withColumn("sourceSystem", lit("editsphere_v1"))
        .withColumn("sourceStation", lit("HBTV"))
        .withColumn("name", editName(col("name")))
      pdf.show(5, false)
      println(pdf.count)
      pdf
    }
  }

  val compo2 = new DbToPq with Executor {

    val compoName = ""
    val writerPqPath: String = "E:\\workspace\\SphereSpark\\data\\test/wf_item_newsphere"
    val writeMode = Append
    val sourceStation = new NewSphere
    override val columns = Array("workitemid", "workitemtype", "activitydefname", "activitytmplname", "worker", "currentstate", "createtime", "createdby", "starttime", "stoptime", "workname", "'newsphere_v1'", "'CDV'")
    val readTableName = "wf_workitem"
    //    val readDbWhere = Array(s"stoptime >= '$starttime' and stoptime < '$endtime' and currentstate in (3,4,5)")
    override val readDbWhere = Array(s"currentstate in (3,4,5)")
    //    override val predicates = Array("currentstate =3", "currentstate =4", "currentstate =5")


    def invoke(df: DataFrame)(implicit args: Array[String]) = {
      val pdf = df
        .withColumn("sourceSystem", col("newsphere_v1"))
        .withColumn("sourceStation", col("CDV"))
        .withColumn("name", editName(col("workname")))
        .withColumn("state", editStateCode(col("currentstate")))
      pdf.show(5, false)
      println(pdf.count)
      pdf
    }
  }

  val editName = udf { (name: String) => {
    name match {
      case null => null
      case _ => name.replaceAll("\\||\r|\n", " ")
    }
  }
  }

  val editStateCode = udf { (code: Int) => {
    code match {
      case 3 => "Terminated"
      case 4 => "Completed"
      case 5 => "Exception"
    }
  }
  }
}
