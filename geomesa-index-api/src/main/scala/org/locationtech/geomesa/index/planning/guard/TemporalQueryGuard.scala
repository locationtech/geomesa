/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning.guard

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, Query}
import org.locationtech.geomesa.index.api.QueryStrategy
import org.locationtech.geomesa.index.index.{TemporalIndex, TemporalIndexValues}
import org.locationtech.geomesa.index.planning.QueryInterceptor
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.SimpleFeatureType

import scala.concurrent.duration.Duration
import scala.util.Try

class TemporalQueryGuard extends QueryInterceptor with LazyLogging {

  import org.locationtech.geomesa.index.planning.guard.TemporalQueryGuard.Config

  private var max: Duration = _
  private var disabled: Boolean = false

  override def init(ds: DataStore, sft: SimpleFeatureType): Unit = {
    disabled = TemporalQueryGuard.disabled(sft.getTypeName)
    if (disabled) {
      logger.info(s"This guard is disabled for schema '${sft.getTypeName}' via system property")
    }
    max = Try(Duration(sft.getUserData.get(Config).asInstanceOf[String])).getOrElse {
      throw new IllegalArgumentException(
        s"Temporal query guard expects valid duration under user data key '$Config'")
    }
  }

  override def rewrite(query: Query): Unit = {}

  override def guard(strategy: QueryStrategy): Option[IllegalArgumentException] = {
    val msg = if (disabled || !strategy.index.isInstanceOf[TemporalIndex[_, _]]) { None } else {
      strategy.values.collect { case v: TemporalIndexValues => v.intervals } match {
        case None => Some("Query does not have a temporal filter")
        case Some(i) if !validate(i, max) => Some(s"Query exceeds maximum allowed filter duration of $max")
        case _ => None
      }
    }
    msg.map(m => new IllegalArgumentException(s"$m: ${filterString(strategy)}"))
  }

  override def close(): Unit = {}
}

object TemporalQueryGuard {

  val Config = "geomesa.guard.temporal.max.duration"

  def disabled(typeName: String): Boolean =
    SystemProperty(s"geomesa.guard.temporal.$typeName.disable").toBoolean.contains(true)
}
