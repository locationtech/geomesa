/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

import java.util.Date

import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
  * Configurable feature expiration (time-to-live, age-off)
  */
sealed trait FeatureExpiration {

  /**
    * Returns the expiration time for the feature, in millis since the java epoch
    *
    * @param feature simple feature
    * @return
    */
  def expires(feature: SimpleFeature): Long
}

object FeatureExpiration {

  private val AttributeRegex = """([^()]+)\((.+)\)""".r // name(ttl)

  /**
    * Expiration based on ingest time
    *
    * @param ttl time-to-live after ingestion, before the feature expires
    */
  case class IngestTimeExpiration(ttl: Duration) extends FeatureExpiration {
    private val millis = ttl.toMillis
    override def expires(feature: SimpleFeature): Long = System.currentTimeMillis() + millis
  }

  /**
    * Expiration based on an attribute of the feature
    *
    * @param attribute name of a Date-type attribute
    * @param i index in the sft of the attribute
    * @param ttl time-to-live after the attribute date, before the feature expires
    */
  case class FeatureTimeExpiration(attribute: String, i: Int, ttl: Duration)
      extends FeatureExpiration {
    private val millis = ttl.toMillis
    override def expires(feature: SimpleFeature): Long = {
      val date = feature.getAttribute(i).asInstanceOf[Date]
      if (date == null) { 0L } else { date.getTime + millis }
    }
  }

  /**
    * Parse an expiration string
    *
    * @param sft simple feature type
    * @param expiration expiration string
    * @return
    */
  def apply(sft: SimpleFeatureType, expiration: String): FeatureExpiration = {
    val matcher = AttributeRegex.pattern.matcher(expiration)
    if (matcher.matches()) {
      val attribute = matcher.group(1)
      val i = sft.indexOf(attribute)
      if (i == -1 || !classOf[Date].isAssignableFrom(sft.getDescriptor(i).getType.getBinding)) {
        throw new IllegalArgumentException(s"Invalid age-off attribute: $attribute")
      }
      FeatureTimeExpiration(attribute, i, duration(matcher.group(2)))
    } else {
      IngestTimeExpiration(duration(expiration))
    }
  }

  /**
    * Convert an expiration to a serialized string
    *
    * @param expiration expiration
    * @return
    */
  def unapply(expiration: FeatureExpiration): Option[String] = {
    expiration match {
      case IngestTimeExpiration(duration)                => Some(duration.toString)
      case FeatureTimeExpiration(attribute, _, duration) => Some(s"$attribute($duration)")
      case _                                             => None
    }
  }

  private def duration(string: String): Duration = {
    try {
      val duration = Duration(string)
      if (!duration.isFinite()) {
        throw new IllegalArgumentException("Duration is infinite")
      } else if (duration <= Duration.Zero) {
        throw new IllegalArgumentException("Duration is negative")
      }
      duration
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid age-off time-to-live: $string", e)
    }
  }
}
