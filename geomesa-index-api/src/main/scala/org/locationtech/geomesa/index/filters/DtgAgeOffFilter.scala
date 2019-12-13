/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.filters

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.IteratorCache
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

/**
  * Age-off a feature based on an attribute time
  */
trait DtgAgeOffFilter extends AgeOffFilter with LazyLogging {

  protected var sft: SimpleFeatureType = _
  protected var index: GeoMesaFeatureIndex[_, _] = _

  protected var reusableSf: KryoBufferSimpleFeature = _
  protected var dtgIndex: Int = -1

  override def init(options: Map[String, String]): Unit = {
    import DtgAgeOffFilter.Configuration.{DtgOpt, IndexOpt, SftOpt}

    super.init(options)

    val spec = options(SftOpt)
    sft = IteratorCache.sft(spec)
    index = IteratorCache.index(sft, spec, options(IndexOpt))

    // noinspection ScalaDeprecation
    val withId = if (index.serializedWithId) { SerializationOptions.none } else { SerializationOptions.withoutId }
    reusableSf = IteratorCache.serializer(spec, withId).getReusableFeature
    dtgIndex = options(DtgOpt).toInt // note: keep this last, for back-compatibility with DtgAgeOffIterator
  }

  override def accept(row: Array[Byte],
                      rowOffset: Int,
                      rowLength: Int,
                      value: Array[Byte],
                      valueOffset: Int,
                      valueLength: Int,
                      timestamp: Long): Boolean = {
    try {
      reusableSf.setBuffer(value, valueOffset, valueLength)
      reusableSf.getDateAsLong(dtgIndex) > expiry
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error checking age-off for " +
            Option(value).getOrElse(Array.empty).mkString("[", ",", s"], offset: $valueOffset, length: $valueLength"))
        false
    }
  }
}

object DtgAgeOffFilter {

  // configuration keys
  object Configuration {
    val SftOpt   = "sft"
    val IndexOpt = "index"
    val DtgOpt   = "dtg"
  }

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                expiry: scala.concurrent.duration.Duration,
                dtgField: Option[String]): Map[String, String] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    require(!sft.isTableSharing || SystemProperty("geomesa.age-off.override").option.exists(_.toBoolean),
      "DtgAgeOff filter should only be applied to features that don't use table sharing. You may override this check" +
          "by setting the system property 'geomesa.age-off.override=true', however please note that age-off" +
          "will affect all shared feature types in the same catalog, and may not work correctly with multiple feature" +
          "types")

    val dtgIndex = dtgField match {
      case None =>
        sft.getDtgIndex.getOrElse {
          throw new IllegalArgumentException("Simple feature type does not have a valid date field")
        }
      case Some(dtg) =>
        val i = sft.indexOf(dtg)
        if (i == -1 || !classOf[Date].isAssignableFrom(sft.getDescriptor(i).getType.getBinding)) {
          throw new IllegalArgumentException(s"Simple feature type does not have a valid date field '$dtg'")
        }
        i
    }

    AgeOffFilter.configure(sft, expiry) ++ Map (
      Configuration.SftOpt   -> SimpleFeatureTypes.encodeType(sft),
      Configuration.IndexOpt -> index.identifier,
      Configuration.DtgOpt   -> dtgIndex.toString
    )
  }
}
