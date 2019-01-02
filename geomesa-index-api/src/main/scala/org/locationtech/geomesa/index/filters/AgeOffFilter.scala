/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.filters

import java.time.{Duration, ZonedDateTime}

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Age off a feature based on the key timestamp
  */
trait AgeOffFilter extends AbstractFilter {

  protected var expiry: Long = -1L

  override def init(options: Map[String, String]): Unit = {
    import AgeOffFilter.Configuration.ExpiryOpt
    expiry = ZonedDateTime.now().minus(Duration.parse(options(ExpiryOpt))).toInstant.toEpochMilli
  }

  override def accept(row: Array[Byte],
                      rowOffset: Int,
                      rowLength: Int,
                      value: Array[Byte],
                      valueOffset: Int,
                      valueLength: Int,
                      timestamp: Long): Boolean = timestamp > expiry
}

object AgeOffFilter {

  // configuration keys
  object Configuration {
    val ExpiryOpt = "retention"
  }

  def configure(sft: SimpleFeatureType, expiry: scala.concurrent.duration.Duration): Map[String, String] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    require(!sft.isTableSharing || SystemProperty("geomesa.age-off.override").option.exists(_.toBoolean),
      "AgeOff filter should only be applied to features that don't use table sharing. You may override this check" +
          "by setting the system property 'geomesa.age-off.override=true', however please note that age-off" +
          "will affect all shared feature types in the same catalog")
    // we use java.time.Duration.toString to serialize as ISO-8601 to not break compatibility
    Map(Configuration.ExpiryOpt -> Duration.ofMillis(expiry.toMillis).toString)
  }
}
