/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.stats

import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.Query
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.{DateTimeZone, Interval}
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.tables.RecordTable
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, wholeWorldEnvelope}

/**
 * Tracks stats for a schema - spatial/temporal bounds, number of records, etc. Persistence of
 * stats is not part of this trait, as different implementations will likely have different method signatures.
 */
trait GeoMesaStats {

  /**
   * Get rough bounds for a query
   *
   * @param query query
   * @return bounds
   */
  def estimateBounds(query: Query): ReferencedEnvelope

  /**
   * Get rough time bounds for a query
   *
   * @param query query
   * @return time bounds
   */
  def estimateTimeBounds(query: Query): Interval

  /**
   * Rough estimate of number of features
   *
   * @param query query
   * @return count of features
   */
  def estimateCount(query: Query): Long
}

/**
 * Tracks stats via entries stored in metadata
 */
trait GeoMesaMetadataStats extends GeoMesaStats {

  this: AccumuloConnectorCreator with HasGeoMesaMetadata =>

  import GeoMesaStats.{allTimeBounds, decodeSpatialBounds, decodeTimeBounds, encode}

  /**
   * Get rough bounds for a query
   * Note: we don't currently filter by the cql
   *
   * @param query query
   * @return bounds
   */
  override def estimateBounds(query: Query): ReferencedEnvelope =
    readSpatialBounds(query.getTypeName)
        .map(new ReferencedEnvelope(_, CRS_EPSG_4326))
        .getOrElse(wholeWorldEnvelope)

  /**
   * Get rough time bounds for a query
   * Note: we don't currently filter by the cql
   *
   * @param query query
   * @return time bounds
   */
  override def estimateTimeBounds(query: Query): Interval =
    readTimeBounds(query.getTypeName).getOrElse(allTimeBounds)

  /**
   * Rough estimate of number of features
   * Note: we don't currently filter by the cql
   *
   * @param query query
   * @return count of features
   */
  override def estimateCount(query: Query): Long =
    metadata.getTableSize(getTableName(query.getTypeName, RecordTable))

  /**
   * Clears any existing spatial bounds
   *
   * @param typeName simple feature type
   */
  def clearSpatialBounds(typeName: String): Unit = metadata.insert(typeName, SPATIAL_BOUNDS_KEY, "")

  /**
   * Clears any existing temporal bounds
   *
   * @param typeName simple feature type
   */
  def clearTemporalBounds(typeName: String): Unit = metadata.insert(typeName, TEMPORAL_BOUNDS_KEY, "")

  /**
   * Writes spatial bounds for this feature
   *
   * @param typeName simple feature type
   * @param bounds partial bounds - existing bounds will be expanded to include this
   */
  def writeSpatialBounds(typeName: String, bounds: Envelope): Unit = {
    val toWrite = readSpatialBounds(typeName) match {
      case None => Some(bounds)
      case Some(current) =>
        val expanded = new Envelope(current)
        expanded.expandToInclude(bounds)
        if (current == expanded) None else Some(expanded)
    }
    toWrite.foreach(b => metadata.insert(typeName, SPATIAL_BOUNDS_KEY, encode(b)))
  }

  /**
   * Writes temporal bounds for this feature
   *
   * @param typeName simple feature type
   * @param bounds partial bounds - existing bounds will be expanded to include this
   */
  def writeTemporalBounds(typeName: String, bounds: Interval): Unit = {
    import org.locationtech.geomesa.utils.time.Time.RichInterval
    val toWrite = readTimeBounds(typeName) match {
      case None => Some(bounds)
      case Some(current) =>
        val expanded = current.expandByInterval(bounds)
        if (current == expanded) None else Some(expanded)
    }
    toWrite.foreach(b => metadata.insert(typeName, TEMPORAL_BOUNDS_KEY, encode(b)))
  }

  private def readTimeBounds(typeName: String): Option[Interval] =
    metadata.readNoCache(typeName, TEMPORAL_BOUNDS_KEY).filterNot(_.isEmpty).map(decodeTimeBounds)

  private def readSpatialBounds(typeName: String): Option[Envelope] =
    metadata.readNoCache(typeName, SPATIAL_BOUNDS_KEY).filterNot(_.isEmpty).map(decodeSpatialBounds)
}

object GeoMesaStats {

  def allTimeBounds = new Interval(0L, System.currentTimeMillis(), DateTimeZone.UTC) // Epoch till now

  def decodeTimeBounds(value: String): Interval = {
    val longs = value.split(":").map(java.lang.Long.parseLong)
    require(longs(0) <= longs(1))
    require(longs.length == 2)
    new Interval(longs(0), longs(1), DateTimeZone.UTC)
  }

  def decodeSpatialBounds(string: String): Envelope = {
    val minMaxXY = string.split(":")
    require(minMaxXY.size == 4)
    new Envelope(minMaxXY(0).toDouble, minMaxXY(1).toDouble, minMaxXY(2).toDouble, minMaxXY(3).toDouble)
  }

  def encode(bounds: Interval): String = s"${bounds.getStartMillis}:${bounds.getEndMillis}"

  def encode(bounds: Envelope): String =
    Seq(bounds.getMinX, bounds.getMaxX, bounds.getMinY, bounds.getMaxY).mkString(":")
}
