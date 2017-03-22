/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geoserver

import java.util.{Locale, Map => jMap}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.index.api.QueryPlanner.CostEvaluation
import org.locationtech.geomesa.index.api.QueryPlanner.CostEvaluation.CostEvaluation
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try
import scala.util.control.NonFatal

object ViewParams extends LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints._

  import scala.collection.JavaConversions._

  private val envelope = """\[\s*(-?\d+(\.\d+)?),\s*(-?\d+(\.\d+)?),\s*(-?\d+(\.\d+)?),\s*(-?\d+(\.\d+)?)\s*]""".r

  /**
    * Examines the view parameters passed in through geoserver and sets the corresponding query hints
    * This kind of a hack, but it's the only way geoserver exposes custom data to the underlying data store.
    *
    * @param ds geomesa data store
    * @param sft simple feature type
    * @param query query to examine/update
    */
  def setHints(ds: GeoMesaDataStore[_, _, _], sft: SimpleFeatureType, query: Query): Unit = {
    val params = {
      val viewParams = query.getHints.get(Hints.VIRTUAL_TABLE_PARAMETERS).asInstanceOf[jMap[String, String]]
      Option(viewParams).map(_.toMap).getOrElse(Map.empty)
    }

    // note: keys in the map are always uppercase.
    params.foreach { case (key, value) =>
      val setHint = setQueryHint(query, key) _
      key match {
        case "QUERY_INDEX"     => toIndex(ds, sft, value).foreach(setHint(QUERY_INDEX, _))
        case "COST_EVALUATION" => toCost(value).foreach(setHint(COST_EVALUATION, _))

        case "DENSITY_BBOX"    => toEnvelope(key, value).foreach(setHint(DENSITY_BBOX, _))
        case "DENSITY_WEIGHT"  => setHint(DENSITY_WEIGHT, value)
        case "DENSITY_WIDTH"   => toInt(key, value).foreach(setHint(DENSITY_WIDTH, _))
        case "DENSITY_HEIGHT"  => toInt(key, value).foreach(setHint(DENSITY_HEIGHT, _))

        case "STATS_STRING "   => setHint(STATS_STRING, value)
        case "ENCODE_STATS"    => toBoolean(key, value).foreach(setHint(ENCODE_STATS, _))

        case "EXACT_COUNT"     => toBoolean(key, value).foreach(setHint(EXACT_COUNT, _))
        case "LOOSE_BBOX"      => toBoolean(key, value).foreach(setHint(LOOSE_BBOX, _))

        case "SAMPLING"        => toFloat(key, value).foreach(setHint(SAMPLING, _))
        case "SAMPLE_BY"       => setHint(SAMPLE_BY, value)

        case "BIN_TRACK"       => setHint(BIN_TRACK, value)
        case "BIN_GEOM"        => setHint(BIN_GEOM, value)
        case "BIN_DTG"         => setHint(BIN_DTG, value)
        case "BIN_LABEL"       => setHint(BIN_LABEL, value)
        case "BIN_SORT"        => toBoolean(key, value).foreach(setHint(BIN_SORT, _))
        case "BIN_BATCH_SIZE"  => toInt(key, value).foreach(setHint(BIN_BATCH_SIZE, _))

        // back-compatible check for strategy
        case "STRATEGY"        => toIndex(ds, sft, value).foreach(setHint(QUERY_INDEX, _))

        case _ => logger.debug(s"Ignoring view param $key=$value")
      }
    }
  }

  private def setQueryHint(query: Query, name: String)(hint: Hints.Key, value: Any): Unit = {
    val old = query.getHints.get(hint)
    if (old == null) {
      logger.debug(s"Using query hint from geoserver view params: $name=$value")
      query.getHints.put(hint, value)
    } else if (old != value) {
      logger.warn("Ignoring query hint from geoserver in favor of hint directly set in query. " +
          s"Using $name=$old and disregarding $value")
    }
  }

  private def toIndex(ds: GeoMesaDataStore[_, _, _],
                      sft: SimpleFeatureType,
                      name: String): Option[GeoMesaFeatureIndex[_, _, _]] = {
    val check = name.toLowerCase(Locale.US)
    val rawIndices = ds.manager.indices(sft, IndexMode.Read)
    val indices = rawIndices.asInstanceOf[Seq[GeoMesaFeatureIndex[_ <: GeoMesaDataStore[_, _, _], _ <: WrappedFeature, _]]]
    val value = if (check.contains(":")) {
      indices.find(_.identifier.toLowerCase(Locale.US) == check)
    } else {
      indices.find(_.name.toLowerCase(Locale.US) == check)
    }
    if (value.isEmpty) {
      logger.error(s"Ignoring invalid strategy name from view params: $name. Valid values " +
          s"are ${indices.map(i => s"${i.name}, ${i.identifier}").mkString(", ")}")
    }
    value
  }

  private def toCost(name: String): Option[CostEvaluation] = {
    val check = name.toLowerCase(Locale.US)
    val value = CostEvaluation.values.find(_.toString.toLowerCase(Locale.US) == check)
    if (value.isEmpty) {
      logger.error(s"Ignoring invalid cost type from view params: $name. Valid values " +
          s"are ${CostEvaluation.values.mkString(", ")}")
    }
    value
  }

  private def toEnvelope(name: String, geom: String): Option[ReferencedEnvelope] = {
    import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326

    def fromBbox: Try[ReferencedEnvelope] = Try {
      // note: underlines are internal capture groups enclosed in the values we're already pulling out
      val envelope(xmin, _, ymin, _, xmax, _, ymax, _) = geom
      new ReferencedEnvelope(xmin.toDouble, xmax.toDouble, ymin.toDouble, ymax.toDouble, CRS_EPSG_4326)
    }

    def fromWkt: Try[ReferencedEnvelope] =
      Try(new ReferencedEnvelope(WKTUtils.read(geom).getEnvelopeInternal, CRS_EPSG_4326))

    val value = fromBbox.orElse(fromWkt).toOption
    if (value.isEmpty) {
      logger.error(s"Ignoring invalid envelope from view params: $name=$geom. Envelope should be " +
          "WKT or in the form [-180.0,-90.0,180.0,90.0]")
    }
    value
  }

  private def toInt(name: String, int: String): Option[Int] = {
    try {
      Some(int.toInt)
    } catch {
      case NonFatal(e) => logger.error(s"Ignoring invalid int type from view params: $name=$int"); None
    }
  }

  private def toFloat(name: String, float: String): Option[Float] = {
    try {
      Some(float.toFloat)
    } catch {
      case NonFatal(e) => logger.error(s"Ignoring invalid float type from view params: $name=$float"); None
    }
  }

  private def toBoolean(name: String, bool: String): Option[Boolean] = {
    try {
      Some(bool.toBoolean)
    } catch {
      case NonFatal(e) => logger.error(s"Ignoring invalid int type from view params: $name=$bool"); None
    }
  }
}
