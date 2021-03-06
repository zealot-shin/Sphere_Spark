package app.common.base

object InArgs {
  def apply(appId: String, fromDate: String, toDate: String, pqPath: String, debug: String) = {
    new InArgs(appId, fromDate, toDate, pqPath, debug)
  }

  def setId(id: String, inArgs: InArgs) = {
    val argsArr: Array[String] = inArgs.copy
    new InArgs(id, argsArr(1), argsArr(2), argsArr(3), argsArr(4))
  }
}

class InArgs(val appId: String,
             val fromDate: String,
             val toDate: String,
             val pqPath: String,
             val debug: String) {
  def copy: Array[String] = {
    Array(this.appId, this.fromDate, this.toDate, this.pqPath, this.debug)
  }
}