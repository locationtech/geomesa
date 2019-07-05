/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.util.Collections

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.geotools.data.{DataStoreFactorySpi, DataStoreFinder, Query}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.memory.cqengine.datastore.GeoCQEngineDataStore
import org.locationtech.geomesa.spark.GeoMesaSparkSQL.GEOMESA_SQL_FEATURE
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.Envelope
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.Try

// the Spark Relation that builds the scan over the GeoMesa table
// used by the SQL optimization rules to push spatio-temporal predicates into the `filt` variable
case class GeoMesaRelation(
    sqlContext: SQLContext,
    sft: SimpleFeatureType,
    schema: StructType,
    params: Map[String, String],
    filt: org.opengis.filter.Filter = org.opengis.filter.Filter.INCLUDE,
    props: Option[Seq[String]] = None,
    var partitionHints : Seq[Int] = null,
    var indexRDD: RDD[GeoCQEngineDataStore] = null,
    var partitionedRDD: RDD[(Int, Iterable[SimpleFeature])] = null,
    var indexPartRDD: RDD[(Int, GeoCQEngineDataStore)] = null)
    extends BaseRelation with PrunedFilteredScan {

  import scala.collection.JavaConverters._

  val cache: Boolean = Try(params("cache").toBoolean).getOrElse(false)
  val indexId: Boolean = Try(params("indexId").toBoolean).getOrElse(false)
  val indexGeom: Boolean  = Try(params("indexGeom").toBoolean).getOrElse(false)
  val numPartitions: Int = Try(params("partitions").toInt).getOrElse(sqlContext.sparkContext.defaultParallelism)
  val spatiallyPartition: Boolean = Try(params("spatial").toBoolean).getOrElse(false)
  val partitionStrategy: String = Try(params("strategy").toString).getOrElse("EQUAL")
  var partitionEnvelopes: List[Envelope] = _
  val providedBounds: String = Try(params("bounds").toString).getOrElse(null)
  val coverPartition: Boolean = Try(params("cover").toBoolean).getOrElse(false)
  // Control partitioning strategies that require a sample of the data
  val sampleSize: Int = Try(params("sampleSize").toInt).getOrElse(100)
  val thresholdMultiplier: Double = Try(params("threshold").toDouble).getOrElse(0.3)

  val initialQuery: String = Try(params("query").toString).getOrElse("INCLUDE")
  val geometryOrdinal: Int = sft.indexOf(sft.getGeometryDescriptor.getLocalName)

  lazy val rawRDD: SpatialRDD = buildRawRDD

  def buildRawRDD: SpatialRDD = {
    val spark = GeoMesaSpark(params.asJava.asInstanceOf[java.util.Map[String, java.io.Serializable]])
    val query = new Query(params(GEOMESA_SQL_FEATURE), ECQL.toFilter(initialQuery))
    val raw = spark.rdd(new Configuration(), sqlContext.sparkContext, params, query)

    if (raw.getNumPartitions != numPartitions && params.contains("partitions")) {
      SpatialRDD(raw.repartition(numPartitions), raw.schema)
    } else {
      raw
    }
  }

  val encodedSFT: String = SimpleFeatureTypes.encodeType(sft, includeUserData = true)

  if (partitionedRDD == null && spatiallyPartition) {
    if (partitionEnvelopes == null) {
      val bounds: Envelope = if (providedBounds == null) {
        RelationUtils.getBound(rawRDD)
      } else {
        WKTUtils.read(providedBounds).getEnvelopeInternal
      }
      partitionEnvelopes = partitionStrategy match {
        case "EARTH" => RelationUtils.wholeEarthPartitioning(numPartitions)
        case "EQUAL" => RelationUtils.equalPartitioning(bounds, numPartitions)
        case "WEIGHTED" => RelationUtils.weightedPartitioning(rawRDD, bounds, numPartitions, sampleSize)
        case "RTREE" => RelationUtils.rtreePartitioning(rawRDD, numPartitions, sampleSize, thresholdMultiplier)
        case _ => throw new IllegalArgumentException(s"Invalid partitioning strategy specified: $partitionStrategy")
      }
    }
    partitionedRDD = RelationUtils.spatiallyPartition(partitionEnvelopes, rawRDD, numPartitions, geometryOrdinal)
    partitionedRDD.persist(StorageLevel.MEMORY_ONLY)
  }

  if (cache) {
    def isCqStore(spi: DataStoreFactorySpi): Boolean = spi.canProcess(Collections.singletonMap("cqengine", "true"))
    if (!DataStoreFinder.getAvailableDataStores.asScala.exists(isCqStore)) {
      throw new IllegalArgumentException("Cache argument set to true but GeoCQEngineDataStore is not on the classpath")
    }
    if (spatiallyPartition && indexPartRDD == null) {
      indexPartRDD = RelationUtils.indexPartitioned(encodedSFT, sft.getTypeName, partitionedRDD, indexId, indexGeom)
      partitionedRDD.unpersist() // make this call blocking?
      indexPartRDD.persist(StorageLevel.MEMORY_ONLY)
    } else if (indexRDD == null) {
      indexRDD = RelationUtils.index(encodedSFT, sft.getTypeName, rawRDD, indexId, indexGeom)
      indexRDD.persist(StorageLevel.MEMORY_ONLY)
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[org.apache.spark.sql.sources.Filter]): RDD[Row] = {
    if (cache) {
      if (spatiallyPartition) {
        RelationUtils.buildScanInMemoryPartScan(requiredColumns, filters, filt,
          sqlContext.sparkContext, schema, params, partitionHints, indexPartRDD)
      } else {
        RelationUtils.buildScanInMemoryScan(requiredColumns, filters, filt, sqlContext.sparkContext, schema, params, indexRDD)
      }
    } else {
      RelationUtils.buildScan(requiredColumns, filters, filt, sqlContext.sparkContext, schema, params)
    }
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter {
      case t @ (_:IsNotNull | _:IsNull) => true
      case _ => false
    }
  }
}
