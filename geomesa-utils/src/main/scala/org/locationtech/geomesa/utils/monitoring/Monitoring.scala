/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.monitoring

import com.google.gson.{Gson, GsonBuilder}
import com.typesafe.scalalogging.LazyLogging

object Monitoring extends LazyLogging {
  private val gson: Gson = new GsonBuilder()
    .serializeNulls()
    .create()

  def log(stat: UsageStat): Unit = {
    logger.trace(gson.toJson(stat))
  }
}

trait UsageStat {
  def storeType: String
  def typeName: String
  def date: Long
}


