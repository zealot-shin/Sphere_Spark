package testCompo

import app.common.base.SparkApp
import app.common.executor.Executor
import app.common.template.DbToPq
import org.apache.spark.sql.DataFrame

object TestCompo extends SparkApp{
  def exec(implicit args: Array[String]) = {
    compo.run()
  }

  val compo = new DbToPq with Executor {
    def invoke(df:DataFrame)(implicit args: Array[String]) = {
      df.show(5,false)
      println(df.count)
      df
    }
  }
}
