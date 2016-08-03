package org.locationtech.geomesa.utils.geotools

import com.google.gson.{Gson, GsonBuilder}
import com.typesafe.scalalogging.LazyLogging

object Monitoring extends LazyLogging {
  private val gson: Gson = new GsonBuilder()
    .serializeNulls()
    .create()

  def log(stat: UsageStat): Unit = {
    println("Logging a message")
    logger.trace(gson.toJson(stat))
  }
}

trait UsageStat {
  def storeType: String
  def typeName: String
  def date: Long
}


