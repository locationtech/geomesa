/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.audit

import org.geotools.factory.Hints
import org.locationtech.geomesa.utils.audit.{AuditedEvent, DeletableEvent}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

case class QueryEvent(storeType: String,
                      typeName: String,
                      date:     Long,
                      user:     String,
                      filter:   String,
                      hints:    String,
                      planTime: Long,
                      scanTime: Long,
                      hits:     Long,
                      deleted:  Boolean = false) extends AuditedEvent with DeletableEvent

object QueryEvent {

  import org.locationtech.geomesa.index.conf.QueryHints._
  import org.locationtech.geomesa.index.conf.QueryHints.Internal._

  // list of query hints we want to persist
  val QUERY_HINTS =
    List[Hints.Key](TRANSFORMS, TRANSFORM_SCHEMA, DENSITY_BBOX, DENSITY_WIDTH, DENSITY_HEIGHT, BIN_TRACK, STATS_STRING)

  /**
    * Converts a query hints object to a string for persisting
    *
    * @param hints hints to transform
    * @return
    */
  def hintsToString(hints: Hints): String =
    QUERY_HINTS.flatMap(k => Option(hints.get(k)).map(v => hintToString(k, v))).sorted.mkString(",")

  /**
    * Converts a single hint to a string
    */
  private def hintToString(key: Hints.Key, value: AnyRef): String = s"${keyToString(key)}=${valueToString(value)}"

  /**
    * Maps a query hint to a string. We need this since the classes themselves don't really have a
    * decent toString representation.
    *
    * @param key hints key
    * @return
    */
  def keyToString(key: Hints.Key): String = key match {
    // note: keep these at their old values for back compatibility
    case TRANSFORMS       => "TRANSFORMS"
    case TRANSFORM_SCHEMA => "TRANSFORM_SCHEMA"
    case BIN_TRACK        => "BIN_TRACK_KEY"
    case STATS_STRING     => "STATS_STRING_KEY"
    case ENCODE_STATS     => "RETURN_ENCODED"
    case DENSITY_BBOX     => "DENSITY_BBOX_KEY"
    case DENSITY_WIDTH    => "WIDTH_KEY"
    case DENSITY_HEIGHT   => "HEIGHT_KEY"
    case _                => "unknown_hint"
  }

  /**
    * Encodes a value into a decent readable string
    */
  private def valueToString(value: AnyRef): String = value match {
    case null => "null"
    case sft: SimpleFeatureType => s"[${sft.getTypeName}]${SimpleFeatureTypes.encodeType(sft)}"
    case v => v.toString
  }
}
