/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.sql

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.FilterFactory
import org.geotools.factory.CommonFactoryFinder
import org.locationtech.geomesa.spark.SpatialRDD
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry}
import org.locationtech.jts.index.strtree.{AbstractNode, Boundable, STRtree}

import scala.collection.mutable.ListBuffer

object RelationUtils extends LazyLogging {

  import scala.collection.JavaConverters._

  @transient val ff: FilterFactory = CommonFactoryFinder.getFilterFactory

  implicit val CoordinateOrdering: Ordering[Coordinate] = Ordering.by {_.x}

  def grid(rdd: SpatialRDD, envelopes: List[Envelope], parallelism: Int): RDD[(Int, Iterable[SimpleFeature])] = {
    val geom = rdd.schema.indexOf(rdd.schema.getGeometryDescriptor.getLocalName)
    rdd.flatMap(RelationUtils.gridIdMapper(_, envelopes, geom)).groupByKey(new IndexPartitioner(parallelism))
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
    val sample = rawRDD.takeSample(withReplacement = false, sampleSize)
    val binSize = sample.length / width
    if (binSize > 0)  {
      val xSample = sample.map{f => f.getDefaultGeometry.asInstanceOf[Geometry].getCoordinates.min.x}
      val ySample = sample.map{f => f.getDefaultGeometry.asInstanceOf[Geometry].getCoordinates.min.y}
      val xSorted = xSample.sorted
      val ySorted = ySample.sorted
      val partitionEnvelopes: ListBuffer[Envelope] = ListBuffer()
      for (xBin <- 0 until width) {
        val minX = xSorted(xBin * binSize)
        val maxX = xSorted(math.min((xBin + 1) * binSize, xSorted.length - 1))
        for (yBin <- 0 until width) {
          val minY = ySorted(yBin * binSize)
          val maxY = ySorted(math.min((yBin + 1) * binSize, ySorted.length - 1))
          partitionEnvelopes += new Envelope(minX, maxX, minY, maxY)
        }
      }
      partitionEnvelopes.toList
    } else List(bound)
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
}
