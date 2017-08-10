package testCompo

import app.common.base.SparkApp
import app.common.executor.Executor
import app.common.template.DbToPq
import org.apache.spark.sql.DataFrame
import spark.common.util.EditSphere

object TestCompo extends SparkApp{
  def exec(implicit args: Array[String]) = {
    compo.run()
  }

  val compo = new DbToPq with Executor {

     lazy val sourceStation =  new EditSphere
     lazy val readTableName = "nSite.wf.workitems"
     lazy val readDatabase = "nsitedb"

    def invoke(df:DataFrame)(implicit args: Array[String]) = {
      df.show(5,false)
      println(df.count)
      df
    }
  }
}
