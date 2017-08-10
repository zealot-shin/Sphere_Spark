package spark.common

import org.slf4j.LoggerFactory

trait Logging extends Serializable{
  val logger = LoggerFactory.getLogger(this.getClass)

  def platformError(t: Throwable) = logger.error("[PLATFORM]",t)
  def appError(t : Throwable)  = logger.error("[APP]",t)

  def isDebugEnabled = logger.isDebugEnabled
}