/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{IndexPartitioner, Row}
import org.geotools.data.{Query, Transaction}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.memory.cqengine.datastore.GeoCQEngineDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry}
import org.locationtech.jts.index.strtree.{AbstractNode, Boundable, STRtree}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.FilterFactory2

import scala.collection.Iterator
import scala.collection.mutable.ListBuffer

object RelationUtils extends LazyLogging {

  import CaseInsensitiveMapFix._

  import scala.collection.JavaConverters._

  @transient val ff: FilterFactory2 = CommonFactoryFinder.getFilterFactory2

  implicit val CoordinateOrdering: Ordering[Coordinate] = Ordering.by {_.x}

  def indexIterator(sft: SimpleFeatureType, indexId: Boolean, indexGeom: Boolean): GeoCQEngineDataStore = {
    val engineStore = new org.locationtech.geomesa.memory.cqengine.datastore.GeoCQEngineDataStore(indexGeom)
    engineStore.createSchema(sft)
    engineStore
  }

  def index(encodedSft: String, typeName: String, rdd: RDD[SimpleFeature], indexId: Boolean, indexGeom: Boolean): RDD[GeoCQEngineDataStore] = {
    rdd.mapPartitions { iter =>
      val sft = SimpleFeatureTypes.createType(typeName,encodedSft)
      val engineStore = RelationUtils.indexIterator(sft, indexId, indexGeom)
      engineStore.namesToEngine.get(typeName).insert(iter.toList)
      Iterator(engineStore)
    }
  }

  def indexPartitioned(encodedSft: String,
      typeName: String,
      rdd: RDD[(Int, Iterable[SimpleFeature])],
      indexId: Boolean,
      indexGeom: Boolean): RDD[(Int, GeoCQEngineDataStore)] = {
    rdd.mapValues { iter =>
      val sft = SimpleFeatureTypes.createType(typeName,encodedSft)
      val engineStore = RelationUtils.indexIterator(sft, indexId, indexGeom)
      engineStore.namesToEngine.get(typeName).insert(iter)
      engineStore
    }
  }

  // Maps a SimpleFeature to the id of the envelope that contains it
  // Will duplicate features that belong to more than one envelope
  // Returns -1 if no match was found
  // TODO: Filter duplicates when querying
  def gridIdMapper(sf: SimpleFeature, envelopes: List[Envelope], geometryOrdinal: Int): List[(Int, SimpleFeature)] = {
    val geom = sf.getAttribute(geometryOrdinal).asInstanceOf[Geometry]
    val mappings = envelopes.indices.flatMap { index =>
      if (envelopes(index).intersects(geom.getEnvelopeInternal)) {
        Some(index, sf)
      } else {
        None
      }
    }
    if (mappings.isEmpty) {
      List((-1, sf))
    } else {
      mappings.toList
    }
  }

  // Maps a geometry to the id of the envelope that contains it
  // Used to derive partition hints
  def gridIdMapper(geom: Geometry, envelopes: List[Envelope]): List[Int] = {
    val mappings = envelopes.indices.flatMap { index =>
      if (envelopes(index).intersects(geom.getEnvelopeInternal)) {
        Some(index)
      } else {
        None
      }
    }
    if (mappings.isEmpty) {
      List(-1)
    } else {
      mappings.toList
    }
  }

  def spatiallyPartition(envelopes: List[Envelope],
      rdd: RDD[SimpleFeature],
      numPartitions: Int,
      geometryOrdinal: Int): RDD[(Int, Iterable[SimpleFeature])] = {
    val keyedRdd = rdd.flatMap { gridIdMapper( _, envelopes, geometryOrdinal)}
    keyedRdd.groupByKey(new IndexPartitioner(numPartitions))
  }

  def getBound(rdd: RDD[SimpleFeature]): Envelope = {
    rdd.aggregate[Envelope](new Envelope())(
      (env: Envelope, sf: SimpleFeature) => {
        env.expandToInclude(sf.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal)
        env
      },
      (env1: Envelope, env2: Envelope) => {
        env1.expandToInclude(env2)
        env1
      }
    )
  }

  def equalPartitioning(bound: Envelope, numPartitions: Int): List[Envelope] = {
    // Compute bounds of each partition
    val partitionsPerDim = Math.sqrt(numPartitions).toInt
    val partitionWidth = bound.getWidth / partitionsPerDim
    val partitionHeight = bound.getHeight / partitionsPerDim
    val minX = bound.getMinX
    val minY = bound.getMinY
    val partitionEnvelopes: ListBuffer[Envelope] = ListBuffer()

    // Build partitions
    for (xIndex <- 0 until partitionsPerDim) {
      val xPartitionStart = minX + (xIndex * partitionWidth)
      val xPartitionEnd = xPartitionStart + partitionWidth
      for (yIndex <- 0 until partitionsPerDim) {
        val yPartitionStart = minY + (yIndex * partitionHeight)
        val yPartitionEnd = yPartitionStart+ partitionHeight
        partitionEnvelopes += new Envelope(xPartitionStart, xPartitionEnd, yPartitionStart, yPartitionEnd)
      }
    }
    partitionEnvelopes.toList
  }

  def weightedPartitioning(rawRDD: RDD[SimpleFeature], bound: Envelope, numPartitions: Int, sampleSize: Int): List[Envelope] = {
    val width: Int = Math.sqrt(numPartitions).toInt
    val binSize = sampleSize / width
    val sample = rawRDD.takeSample(withReplacement = false, sampleSize)
    val xSample = sample.map{f => f.getDefaultGeometry.asInstanceOf[Geometry].getCoordinates.min.x}
    val ySample = sample.map{f => f.getDefaultGeometry.asInstanceOf[Geometry].getCoordinates.min.y}
    val xSorted = xSample.sorted
    val ySorted = ySample.sorted

    val partitionEnvelopes: ListBuffer[Envelope] = ListBuffer()

    for (xBin <- 0 until width) {
      val minX = xSorted(xBin * binSize)
      val maxX = xSorted(((xBin + 1) * binSize) - 1)
      for (yBin <- 0 until width) {
        val minY = ySorted(yBin)
        val maxY = ySorted(((yBin + 1) * binSize) - 1)
        partitionEnvelopes += new Envelope(minX, maxX, minY, maxY)
      }
    }

    partitionEnvelopes.toList
  }

  def wholeEarthPartitioning(numPartitions: Int): List[Envelope] = {
    equalPartitioning(new Envelope(-180,180,-90,90), numPartitions)
  }

  // Constructs an RTree based on a sample of the data and returns its bounds as envelopes
  // returns one less envelope than requested to account for the catch-all envelope
  def rtreePartitioning(
      rawRDD: RDD[SimpleFeature],
      numPartitions: Int,
      sampleSize: Int,
      thresholdMultiplier: Double): List[Envelope] = {
    val sample = rawRDD.takeSample(withReplacement = false, sampleSize)
    val rtree = new STRtree()

    sample.foreach{ sf =>
      rtree.insert(sf.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal, sf)
    }
    val envelopes: java.util.List[Envelope] = new java.util.ArrayList[Envelope]()

    // get rtree envelopes, limited to those containing reasonable size
    val reasonableSize = sampleSize / numPartitions
    val threshold = (reasonableSize * thresholdMultiplier).toInt
    val minSize = reasonableSize - threshold
    val maxSize = reasonableSize + threshold
    rtree.build()
    queryBoundary(rtree.getRoot, envelopes, minSize, maxSize)
    envelopes.asScala.take(numPartitions - 1).toList
  }

  // Helper method to get the envelopes of an RTree
  def queryBoundary(node: AbstractNode, boundaries: java.util.List[Envelope], minSize: Int, maxSize: Int): Int =  {
    // get node's immediate children
    val childBoundables: java.util.List[_] = node.getChildBoundables

    // True if current node is leaf
    var flagLeafnode = true
    var i = 0
    while (i < childBoundables.size && flagLeafnode) {
      val childBoundable = childBoundables.get(i).asInstanceOf[Boundable]
      if (childBoundable.isInstanceOf[AbstractNode]) {
        flagLeafnode = false
      }
      i += 1
    }

    if (flagLeafnode) {
      childBoundables.size
    } else {
      var nodeCount = 0
      for ( i <- 0 until childBoundables.size ) {
        val childBoundable = childBoundables.get(i).asInstanceOf[Boundable]
        childBoundable match {
          case child: AbstractNode =>
            val childSize = queryBoundary(child, boundaries, minSize, maxSize)
            // check boundary for size and existence in chosen boundaries
            if (childSize < maxSize && childSize > minSize) {
              var alreadyAdded = false
              if (node.getLevel != 1) {
                child.getChildBoundables.asInstanceOf[java.util.List[AbstractNode]].asScala.foreach { c =>
                  alreadyAdded = alreadyAdded || boundaries.contains(c.getBounds.asInstanceOf[Envelope])
                }
              }
              if (!alreadyAdded) {
                boundaries.add(child.getBounds.asInstanceOf[Envelope])
              }
            }
            nodeCount += childSize

          case _ => nodeCount += 1 // negligible difference but accurate
        }
      }
      nodeCount
    }
  }

  def coverPartitioning(dataRDD: RDD[SimpleFeature], coverRDD: RDD[SimpleFeature], numPartitions: Int): List[Envelope] = {
    coverRDD.map {
      _.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal
    }.collect().toList
  }

  def buildScan(requiredColumns: Array[String],
      filters: Array[org.apache.spark.sql.sources.Filter],
      filt: org.opengis.filter.Filter,
      ctx: SparkContext,
      schema: StructType,
      params: Map[String, String]): RDD[Row] = {
    logger.debug(
      s"""Building scan, filt = $filt,
         |filters = ${filters.mkString(",")},
         |requiredColumns = ${requiredColumns.mkString(",")}""".stripMargin)
    val compiledCQL = filters.flatMap(SparkUtils.sparkFilterToCQLFilter).foldLeft[org.opengis.filter.Filter](filt) { (l, r) => ff.and(l, r) }
    val requiredAttributes = requiredColumns.filterNot(_ == "__fid__")
    val rdd = GeoMesaSpark(params).rdd(
      new Configuration(ctx.hadoopConfiguration), ctx, params,
      new Query(params(GeoMesaSparkSQL.GEOMESA_SQL_FEATURE), compiledCQL, requiredAttributes))

    val extractors = SparkUtils.getExtractors(requiredColumns, schema)
    val result = rdd.map(SparkUtils.sf2row(schema, _, extractors))
    result.asInstanceOf[RDD[Row]]
  }

  def buildScanInMemoryScan(requiredColumns: Array[String],
      filters: Array[org.apache.spark.sql.sources.Filter],
      filt: org.opengis.filter.Filter,
      ctx: SparkContext,
      schema: StructType,
      params: Map[String, String],  indexRDD: RDD[GeoCQEngineDataStore]): RDD[Row] = {
    logger.debug(
      s"""Building in-memory scan, filt = $filt,
         |filters = ${filters.mkString(",")},
         |requiredColumns = ${requiredColumns.mkString(",")}""".stripMargin)
    val compiledCQL = filters.flatMap(SparkUtils.sparkFilterToCQLFilter).foldLeft[org.opengis.filter.Filter](filt) { (l, r) => SparkUtils.ff.and(l, r) }
    val filterString = ECQL.toCQL(compiledCQL)

    val extractors = SparkUtils.getExtractors(requiredColumns, schema)

    val requiredAttributes = requiredColumns.filterNot(_ == "__fid__")
    val result = indexRDD.flatMap { engine =>
      val cqlFilter = ECQL.toFilter(filterString)
      val query = new Query(params(GeoMesaSparkSQL.GEOMESA_SQL_FEATURE), cqlFilter, requiredAttributes)
      SelfClosingIterator(engine.getFeatureReader(query, Transaction.AUTO_COMMIT))
    }.map(SparkUtils.sf2row(schema, _, extractors))

    result.asInstanceOf[RDD[Row]]
  }

  def buildScanInMemoryPartScan(requiredColumns: Array[String],
      filters: Array[org.apache.spark.sql.sources.Filter],
      filt: org.opengis.filter.Filter,
      ctx: SparkContext,
      schema: StructType,
      params: Map[String, String],
      partitionHints: Seq[Int],
      indexPartRDD: RDD[(Int, GeoCQEngineDataStore)]): RDD[Row] = {
    logger.debug(
      s"""Building partitioned in-memory scan, filt = $filt,
         |filters = ${filters.mkString(",")},
         |requiredColumns = ${requiredColumns.mkString(",")}""".stripMargin)
    val compiledCQL = filters.flatMap(SparkUtils.sparkFilterToCQLFilter).foldLeft[org.opengis.filter.Filter](filt) { (l, r) => SparkUtils.ff.and(l,r) }
    val filterString = ECQL.toCQL(compiledCQL)

    // If keys were derived from query, go straight to those partitions
    val reducedRdd =  if (partitionHints != null) {
      indexPartRDD.filter {case (key, _) => partitionHints.contains(key) }
    } else {
      indexPartRDD
    }

    val extractors = SparkUtils.getExtractors(requiredColumns, schema)

    val requiredAttributes = requiredColumns.filterNot(_ == "__fid__")
    val result = reducedRdd.flatMap { case (key, engine) =>
      val cqlFilter = ECQL.toFilter(filterString)
      val query = new Query(params(GeoMesaSparkSQL.GEOMESA_SQL_FEATURE), cqlFilter, requiredAttributes)
      SelfClosingIterator(engine.getFeatureReader(query, Transaction.AUTO_COMMIT))
    }.map(SparkUtils.sf2row(schema, _, extractors))
    result.asInstanceOf[RDD[Row]]
  }
}
