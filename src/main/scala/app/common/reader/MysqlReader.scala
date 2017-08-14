package app.common.reader

import java.util.Properties

import org.apache.spark.sql.DataFrame
import spark.common.SparkContexts._

object MysqlReader {

  def apply(prop: Properties, url: String, table: String, cols: Array[String], readDbWhere: Array[String], predicates: Array[String]) = {
    new MysqlReader(prop, url, table, cols, readDbWhere, predicates)
  }
}

class MysqlReader(val prop: Properties, val url: String, val table: String, val columns: Array[String], val readDbWhere: Array[String], val predicates: Array[String]) {
  def read(): DataFrame = {
    //    sqlContext.read.jdbc(url, table, prop)
    val tbl = if (!columns.isEmpty) s"( select ${columns.mkString(", ")} from ${table} where ${readDbWhere.mkString(" or ")}) as tbl" else table

    if (predicates == null) {
      sqlContext.read.format("jdbc").options(
        Map("url" -> s"${url}?user=${prop.getProperty("user")}&password=${prop.getProperty("password")}",
          "driver" -> prop.getProperty("driver"),
          "dbtable" -> tbl,
          "numPartions" -> "12"
        )
      ).load()
    } else {
      sqlContext.read.jdbc(s"${url}?user=${prop.getProperty("user")}&password=${prop.getProperty("password")}",
        tbl, predicates, prop)
    }
  }

}
