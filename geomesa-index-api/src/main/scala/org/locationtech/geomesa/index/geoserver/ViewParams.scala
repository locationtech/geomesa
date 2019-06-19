/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geoserver

import java.util.{Locale, Map => jMap}

import com.google.common.collect.ImmutableBiMap
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.util.factory.Hints
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation
import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation.CostEvaluation
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.{StringSerialization, WKTUtils}
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try
import scala.util.control.NonFatal

object ViewParams extends LazyLogging {

  import scala.collection.JavaConverters._

  private val QueryHintMap: ImmutableBiMap[String, Hints.Key] = buildHintsMap(QueryHints)

  private val InternalHintMap: ImmutableBiMap[String, Hints.Key] = buildHintsMap(QueryHints.Internal)

  private val AllHintsMap: java.util.Map[String, Hints.Key] = {
    val map = new java.util.HashMap[String, Hints.Key]
    map.putAll(QueryHintMap)
    map.putAll(InternalHintMap)
    map
  }

  private val envelope = """\[\s*(-?\d+(\.\d+)?),\s*(-?\d+(\.\d+)?),\s*(-?\d+(\.\d+)?),\s*(-?\d+(\.\d+)?)\s*]""".r

  /**
    * Serialize hints into a string
    *
    * @param hints hints
    * @return
    */
  def serialize(hints: Hints): String = {
    val map = (QueryHintMap.asScala ++ InternalHintMap.asScala).flatMap { case (name, hint) =>
      Option(hints.get(hint)).flatMap {
        case v: String             => Some(name -> v)
        case v: java.lang.Boolean  => Some(name -> v.toString)
        case v: java.lang.Integer  => Some(name -> v.toString)
        case v: java.lang.Float    => Some(name -> v.toString)
        case v: ReferencedEnvelope => Some(name -> toString(v))
        case v: CostEvaluation     => Some(name -> v.toString)
        case _ => logger.warn(s"Unhandled hint type for '$name'"); None
      }
    }
    StringSerialization.encodeMap(map)
  }

  /**
    * Deserialize hints from a string serialized with `serialize`
    *
    * @param serialized serialized hints string
    * @return
    */
  def deserialize(serialized: String): Hints = {
    val hints = new Hints()
    setHints(hints, StringSerialization.decodeMap(serialized), AllHintsMap)
    hints
  }

  /**
    * Examines the view parameters passed in through geoserver and sets the corresponding query hints
    * This kind of a hack, but it's the only way geoserver exposes custom data to the underlying data store.
    *
    * @param query query to examine/update
    */
  def setHints(query: Query): Unit = {
    val viewParams = query.getHints.get(Hints.VIRTUAL_TABLE_PARAMETERS).asInstanceOf[jMap[String, String]]
    val params = Option(viewParams).map(_.asScala.toMap).getOrElse(Map.empty)
    setHints(query.getHints, params, QueryHintMap)
  }

  /**
    * Maps query hints to readable strings
    *
    * @param query query
    * @return
    */
  def getReadableHints(query: Query): String = {
    val readable = Seq.newBuilder[String]
    readable.sizeHint(query.getHints.size())
    query.getHints.asScala.foreach { case (k: Hints.Key, v) =>
      val key = hintToString(k)
      val value = v match {
        case null => "null"
        case sft: SimpleFeatureType => SimpleFeatureTypes.encodeType(sft)
        case s => s.toString
      }
      readable += s"$key=$value"
    }
    readable.result.sorted.mkString(", ")
  }

  def hintToString(hint: Hints.Key): String = {
    // for reference, old values in case we need them:
    // case BIN_TRACK      => "BIN_TRACK_KEY"
    // case STATS_STRING   => "STATS_STRING_KEY"
    // case ENCODE_STATS   => "RETURN_ENCODED"
    // case DENSITY_BBOX   => "DENSITY_BBOX_KEY"
    // case DENSITY_WIDTH  => "WIDTH_KEY"
    // case DENSITY_HEIGHT => "HEIGHT_KEY"
    Option(QueryHintMap.inverse().get(hint)).orElse(Option(InternalHintMap.inverse().get(hint))).getOrElse("unknown_hint")
  }

  private def setHints(hints: Hints, params: Map[String, String], lookup: java.util.Map[String, Hints.Key]): Unit = {
    params.foreach { case (original, value) =>
      val key = if (original == "STRATEGY") { "QUERY_INDEX" } else { original }
      lookup.get(key) match {
        case null => logger.debug(s"Ignoring view param $key=$value")
        case hint =>
          try {
            hint.getValueClass match {
              case c if c == classOf[String]             => setHint(hints, key, hint, value)
              case c if c == classOf[java.lang.Boolean]  => toBoolean(key, value).foreach(setHint(hints, key, hint, _))
              case c if c == classOf[java.lang.Integer]  => toInt(key, value).foreach(setHint(hints, key, hint, _))
              case c if c == classOf[java.lang.Float]    => toFloat(key, value).foreach(setHint(hints, key, hint, _))
              case c if c == classOf[ReferencedEnvelope] => toEnvelope(key, value).foreach(setHint(hints, key, hint, _))
              case c if c == classOf[CostEvaluation]     => toCost(value).foreach(setHint(hints, key, hint, _))
              case _ => logger.warn(s"Unhandled hint type for '$key'")
            }
          } catch {
            case NonFatal(e) => logger.warn(s"Error invoking query hint for $key=$value", e)
          }
      }
    }
  }

  private def setHint(hints: Hints, name: String, hint: Hints.Key, value: Any): Unit = {
    val old = hints.get(hint)
    if (old == null) {
      logger.debug(s"Using query hint from geoserver view params: $name=$value")
      hints.put(hint, value)
    } else if (old != value) {
      logger.warn("Ignoring query hint from geoserver in favor of hint directly set in query. " +
          s"Using $name=$old and disregarding $value")
    }
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

  private def toString(env: ReferencedEnvelope): String =
    s"[${env.getMinX},${env.getMinY},${env.getMaxX},${env.getMaxY}]"

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
    try { Some(int.toInt) } catch {
      case NonFatal(_) => logger.error(s"Ignoring invalid int type from view params: $name=$int"); None
    }
  }

  private def toFloat(name: String, float: String): Option[Float] = {
    try { Some(float.toFloat) } catch {
      case NonFatal(_) => logger.error(s"Ignoring invalid float type from view params: $name=$float"); None
    }
  }

  private def toBoolean(name: String, bool: String): Option[Boolean] = {
    try { Some(bool.toBoolean) } catch {
      case NonFatal(_) => logger.error(s"Ignoring invalid int type from view params: $name=$bool"); None
    }
  }

  private def buildHintsMap(obj: Object): ImmutableBiMap[String, Hints.Key] = {
    val methods = obj.getClass.getDeclaredMethods.filter { m =>
      m.getParameterCount == 0 && classOf[Hints.Key].isAssignableFrom(m.getReturnType)
    }
    val map = ImmutableBiMap.builder[String, Hints.Key]()
    // note: keys in the view params map are always uppercase
    methods.foreach(m => map.put(m.getName.toUpperCase(Locale.US), m.invoke(obj).asInstanceOf[Hints.Key]))
    map.build()
  }
}
