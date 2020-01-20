/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2017-2020 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.serializer.WritableSerialization
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.jobs.mapreduce.SimpleFeatureSerialization
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.SimpleFeatureType

/**
 * Common place for setting and getting values out of the mapreduce config
 */
object GeoMesaConfigurator {

  object Keys {
    val Prefix: String = "org.locationtech.geomesa"

    val DsOutParams    : String = s"$Prefix.out.params"
    val IndicesOut     : String = s"$Prefix.out.indices"

    val SftNames       : String = s"$Prefix.sfts"
    val Filter         : String = s"$Prefix.filter"
    val ToFeatures     : String = s"$Prefix.to.features"
    val FeatureReducer : String = s"$Prefix.reducer"
    val Sorting        : String = s"$Prefix.sort"
    val Projection     : String = s"$Prefix.reproject"

    val DesiredSplits  : String = s"$Prefix.mapreduce.split.count.strongHint"
    val Serializers    : String = "io.serializations"
  }

  private val WritableSerialization      = classOf[WritableSerialization].getName
  private val SimpleFeatureSerialization = classOf[SimpleFeatureSerialization].getName
  private val SerializationString        = s"$WritableSerialization,$SimpleFeatureSerialization"

  // set/get the connection parameters for an output format
  def setDataStoreOutParams(conf: Configuration, params: Map[String, String]): Unit =
    conf.set(Keys.DsOutParams, StringSerialization.encodeMap(params))
  def getDataStoreOutParams(conf: Configuration): Map[String, String] =
    StringSerialization.decodeMap(conf.get(Keys.DsOutParams))

  def setIndicesOut(conf: Configuration, indices: Seq[String]): Unit =
    conf.set(Keys.IndicesOut, StringSerialization.encodeSeq(indices))
  def getIndicesOut(conf: Configuration): Option[Seq[String]] =
    Option(conf.get(Keys.IndicesOut)).map(StringSerialization.decodeSeq)

  // set/get the cql filter
  def setFilter(conf: Configuration, filter: String): Unit = conf.set(Keys.Filter, filter)
  def getFilter(conf: Configuration): Option[String] = Option(conf.get(Keys.Filter))

  def setResultsToFeatures(conf: Configuration, resultsToFeatures: ResultsToFeatures[_]): Unit = {
    conf.set(Keys.ToFeatures, ResultsToFeatures.serialize(resultsToFeatures))
    setSerialization(conf, resultsToFeatures.schema)
  }
  def getResultsToFeatures[T](conf: Configuration): ResultsToFeatures[T] =
    ResultsToFeatures.deserialize(conf.get(Keys.ToFeatures))

  def setReducer(conf: Configuration, reducer: FeatureReducer): Unit =
    conf.set(Keys.FeatureReducer, FeatureReducer.serialize(reducer))
  def getReducer(conf: Configuration): Option[FeatureReducer] =
    Option(conf.get(Keys.FeatureReducer)).map(FeatureReducer.deserialize)

  def setSorting(conf: Configuration, sort: Seq[(String, Boolean)]): Unit =
    conf.set(Keys.Sorting, StringSerialization.encodeSeq(sort.flatMap { case (f, r) => Seq(f, r.toString) }))
  def getSorting(conf: Configuration): Option[Seq[(String, Boolean)]] = {
    Option(conf.get(Keys.Sorting)).map { s =>
      StringSerialization.decodeSeq(s).grouped(2).map { case Seq(f, r) => (f, r.toBoolean) }.toList
    }
  }

  def setProjection(conf: Configuration, crs: QueryReferenceSystems): Unit =
    conf.set(Keys.Projection, QueryHints.Internal.toProjectionHint(crs))
  def getProjection(conf: Configuration): Option[QueryReferenceSystems] =
    Option(conf.get(Keys.Projection)).map(QueryHints.Internal.fromProjectionHint)

  /**
    * Configure serialization for a simple feature type
    *
    * @param conf conf
    * @param sft simple feature type
    */
  def setSerialization(conf: Configuration, sft: SimpleFeatureType): Unit = {
    // register the feature serializer
    conf.get(Keys.Serializers) match {
      case null => conf.set(Keys.Serializers, SerializationString)
      case existing =>
        if (!existing.contains(SimpleFeatureSerialization)) {
          conf.set(Keys.Serializers, Seq(existing, SimpleFeatureSerialization).mkString(","))
        }
    }

    // set the schema in the config
    val spec = SimpleFeatureTypes.encodeType(sft, includeUserData = true)
    // note: use the spec hash code to ensure that duplicate type names don't conflict
    val key = s"${(sft.getTypeName + spec).hashCode}:${sft.getTypeName}"
    // store the type name and hash under a common key for all types
    conf.get(Keys.SftNames) match {
      case null => conf.set(Keys.SftNames, key)
      case encoded =>
        val existing = StringSerialization.decodeSeq(encoded)
        if (!existing.contains(key)) {
          conf.set(Keys.SftNames, StringSerialization.encodeSeq(existing :+ key))
        }
    }
    // store the spec under the unique key
    conf.set(s"${Keys.SftNames}.$key", spec)
  }

  /**
    * Get the simple feature types configured for serialization, keyed by hash code
    *
    * @param conf conf
    * @return (unique string key for the type, unique hash code for the type, type)
    */
  def getSerialization(conf: Configuration): Seq[(String, Int, SimpleFeatureType)] = {
    Option(conf.get(Keys.SftNames)).map(StringSerialization.decodeSeq).getOrElse(Seq.empty).map { key =>
      val sep = key.indexOf(':')
      val hash = key.substring(0, sep).toInt
      val typeName = key.substring(sep + 1)
      val spec = conf.get(s"${Keys.SftNames}.$key")
      (key, hash, SimpleFeatureTypes.createType(typeName, spec))
    }
  }
}
