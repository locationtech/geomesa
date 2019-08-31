/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeoHash}
import org.locationtech.jts.geom.{Envelope, Geometry, Point}
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.index.sweepline.{SweepLineIndex, SweepLineInterval, SweepLineOverlapAction}
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Try

case class SpatialLookupJoinRelation(sqlContext: SQLContext,
                                     leftRel: GeoMesaRelation,
                                     rightRel: GeoMesaRelation,
                                     schema: StructType,
                                     condition: Expression,
                                     filt: org.opengis.filter.Filter = org.opengis.filter.Filter.INCLUDE,
                                     props: Option[Seq[String]] = None)
  extends BaseRelation with PrunedFilteredScan {

  val (isLeft, leftIndex, rightIndex, function) = SpatialJoinUtils.relationExpress(leftRel, rightRel, condition)
  val extents = SpatialJoinUtils.geoHashPartitioning(leftRel.rawRDD, leftRel.geoHashPrecision, leftIndex)
  leftRel.partitionedRDD = RelationUtils.spatiallyPartition(extents, leftRel.rawRDD, extents.length, leftIndex)
  rightRel.partitionedRDD = RelationUtils.spatiallyPartition(extents, rightRel.rawRDD, extents.length, rightIndex)

  def lookupJoin(isLeft: Boolean, function: (Geometry, Geometry) => Boolean,
                 leftIndex: Int, rightIndex: Int): RDD[(SimpleFeature, SimpleFeature)] = {
    val partitionPairs = leftRel.partitionedRDD.join(rightRel.partitionedRDD)

    partitionPairs.flatMap { case (key, (left, right)) =>
      val lefts = new ListBuffer[SimpleFeature]()
      var rtree = new STRtree()
      var rights = new ListBuffer[SimpleFeature]()
      var count = 0
      left.foreach(f => lefts.append(f))
      right.foreach { f =>
        count += 1
        rtree.insert(f.getAttribute(rightIndex).asInstanceOf[Geometry].getEnvelopeInternal, f)
        if (count <= RelationUtils.treeLimit) rights.append(f)
      }

      if (count > RelationUtils.treeLimit) {
        rtree.build()
        rights = null
        lefts.flatMap { lf =>
          val joins = new ListBuffer[(SimpleFeature, SimpleFeature)]()
          val leftGeo = lf.getAttribute(leftIndex).asInstanceOf[Geometry]
          val query = rtree.query(leftGeo.getEnvelopeInternal).asInstanceOf[java.util.List[SimpleFeature]]
          query.foreach { rf =>
            val rightGeo = rf.getAttribute(rightIndex).asInstanceOf[Geometry]
            if (SpatialJoinUtils.relationJudge(isLeft, function, leftGeo, rightGeo)) {
              joins.append((lf, rf))
            }
          }
          joins.iterator
        }
      } else {
        rtree = null
        lefts.flatMap { lf =>
          val joins = new ListBuffer[(SimpleFeature, SimpleFeature)]()
          val leftGeo = lf.getAttribute(leftIndex).asInstanceOf[Geometry]
          rights.foreach { rf =>
            val rightGeo = rf.getAttribute(rightIndex).asInstanceOf[Geometry]
            if (SpatialJoinUtils.relationJudge(isLeft, function, leftGeo, rightGeo)) {
              joins.append((lf, rf))
            }
          }
          joins.iterator
        }
      }
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[org.apache.spark.sql.sources.Filter]): RDD[Row] = {
    val joinedRows = lookupJoin(isLeft, function, leftIndex, rightIndex)
    SpatialJoinUtils.buildScan(leftRel, rightRel, joinedRows)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter {
      case t@(_: IsNotNull | _: IsNull) => true
      case _ => false
    }
  }
}

case class RangeDistanceJoinRelation(sqlContext: SQLContext,
                                     leftRel: GeoMesaRelation,
                                     rightRel: GeoMesaRelation,
                                     schema: StructType,
                                     condition: Expression,
                                     filt: org.opengis.filter.Filter = org.opengis.filter.Filter.INCLUDE,
                                     props: Option[Seq[String]] = None)
  extends BaseRelation with PrunedFilteredScan {

  val (greatLeftI, greatRightI, greatEqual, greatDis, lessLeftI, lessRightI, lessEqual, lessDis) =
    SpatialJoinUtils.distanceExpress(leftRel, rightRel, condition)
  val useLookup = Try(leftRel.params("lookupJoin").toBoolean).getOrElse(true)
  if (lessDis >= greatDis) {
    val extents = SpatialJoinUtils.circlePartitioning(leftRel.rawRDD, lessDis, lessLeftI)
    leftRel.partitionedRDD = RelationUtils.spatiallyPartition(extents, leftRel.rawRDD, extents.length, lessLeftI, lessDis)
    rightRel.partitionedRDD = RelationUtils.spatiallyPartition(extents, rightRel.rawRDD, extents.length, lessRightI)
  }

  def lookupJoin(greatLeftI: Int, greatRightI: Int,
                 greatEqual: Int, greatDis: Double,
                 lessLeftI: Int, lessRightI: Int,
                 lessEqual: Int, lessDis: Double): RDD[(SimpleFeature, SimpleFeature)] = {
    val partitionPairs = leftRel.partitionedRDD.join(rightRel.partitionedRDD)

    partitionPairs.flatMap { case (key, (left, right)) =>
      val lefts = new ListBuffer[SimpleFeature]()
      var rtree = new STRtree()
      var rights = new ListBuffer[SimpleFeature]()
      var count = 0
      left.foreach(f => lefts.append(f))
      right.foreach { f =>
        count += 1
        rtree.insert(f.getAttribute(lessRightI).asInstanceOf[Geometry].getEnvelopeInternal, f)
        if (count <= RelationUtils.treeLimit) rights.append(f)
      }

      if (count > RelationUtils.treeLimit) {
        rtree.build()
        rights = null
        lefts.flatMap { lf =>
          val joins = new ListBuffer[(SimpleFeature, SimpleFeature)]()
          val leftGeo = lf.getAttribute(lessLeftI).asInstanceOf[Geometry]
          val env = RelationUtils.extractEnvelope(leftGeo.getEnvelopeInternal, lessDis)
          val query = rtree.query(env).asInstanceOf[java.util.List[SimpleFeature]]
          query.foreach { rf =>
            if (SpatialJoinUtils.distanceJudge(greatLeftI, greatRightI, greatEqual, greatDis,
              lessLeftI, lessRightI, lessEqual, lessDis, lf, rf)) {
              joins.append((lf, rf))
            }
          }
          joins.iterator
        }
      } else {
        rtree = null
        lefts.flatMap { lf =>
          val joins = new ListBuffer[(SimpleFeature, SimpleFeature)]()
          rights.foreach { rf =>
            if (SpatialJoinUtils.distanceJudge(greatLeftI, greatRightI, greatEqual, greatDis,
              lessLeftI, lessRightI, lessEqual, lessDis, lf, rf)) {
              joins.append((lf, rf))
            }
          }
          joins.iterator
        }
      }
    }
  }

  def sweepLineJoin(overlapAction: DistanceOverlapAction, buffer: Double,
                    leftIndex: Int, rightIndex: Int): RDD[(SimpleFeature, SimpleFeature)] = {
    val partitionPairs = leftRel.partitionedRDD.join(rightRel.partitionedRDD)

    partitionPairs.flatMap { case (key, (left, right)) =>
      val sweepLineIndex = new SweepLineIndex()
      left.foreach { feature =>
        val env = RelationUtils.extractEnvelope(feature.getAttribute(leftIndex).asInstanceOf[Geometry].getEnvelopeInternal, buffer)
        val interval = new SweepLineInterval(env.getMinX, env.getMaxX, (0, feature))
        sweepLineIndex.add(interval)
      }
      right.foreach { feature =>
        val env = feature.getAttribute(rightIndex).asInstanceOf[Geometry].getEnvelopeInternal
        val interval = new SweepLineInterval(env.getMinX, env.getMaxX, (1, feature))
        sweepLineIndex.add(interval)
      }
      sweepLineIndex.computeOverlaps(overlapAction)
      overlapAction.joins.iterator
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[org.apache.spark.sql.sources.Filter]): RDD[Row] = {
    if (lessDis < greatDis) {
      sqlContext.sparkContext.emptyRDD[Row]
    } else {
      val joinedRows = if (useLookup) {
        lookupJoin(greatLeftI, greatRightI, greatEqual, greatDis,
          lessLeftI, lessRightI, lessEqual, lessDis)
      } else {
        sweepLineJoin(new DistanceOverlapAction(greatLeftI, greatRightI, greatEqual, greatDis,
          lessLeftI, lessRightI, lessEqual, lessDis),
          lessDis, lessLeftI, lessRightI)
      }
      SpatialJoinUtils.buildScan(leftRel, rightRel, joinedRows)
    }
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter {
      case t@(_: IsNotNull | _: IsNull) => true
      case _ => false
    }
  }
}

case class BroadcastSpatialJoinRelation(sqlContext: SQLContext,
                                        leftRel: GeoMesaRelation,
                                        rightRel: GeoMesaRelation,
                                        schema: StructType,
                                        condition: Expression,
                                        isDistance: Boolean,
                                        filt: org.opengis.filter.Filter = org.opengis.filter.Filter.INCLUDE,
                                        props: Option[Seq[String]] = None)
  extends BaseRelation with PrunedFilteredScan {

  def relationScan(sqlContext: SQLContext,
                   leftRel: GeoMesaRelation,
                   rightRel: GeoMesaRelation,
                   condition: Expression): RDD[Row] = {
    val (isLeft, leftIndex, rightIndex, function) = SpatialJoinUtils.relationExpress(leftRel, rightRel, condition)
    val precision = SpatialJoinUtils.getPrecision()
    val length = precision / 5
    val features = leftRel.rawRDD.collect()
    val indices = features.indices.flatMap { i =>
      val geo = features(i).getAttribute(leftIndex).asInstanceOf[Geometry]
      val env = RelationUtils.extractEnvelope(geo.getEnvelopeInternal)
      BoundingBox.getGeoHashesFromBoundingBox(BoundingBox(env), Short.MaxValue, precision)
        .map(gh => (gh.substring(0, length), i))
    }.groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).toList))
    val broadcast = sqlContext.sparkContext.broadcast(
      (features.indices.map(i => (i, features(i))).toMap, indices)
    )

    val joinedRows = rightRel.rawRDD.flatMap { rf =>
      val joins = new ListBuffer[(SimpleFeature, SimpleFeature)]()
      val rightGeo = rf.getAttribute(rightIndex).asInstanceOf[Geometry]
      BoundingBox.getGeoHashesFromBoundingBox(
        BoundingBox(rightGeo.getEnvelopeInternal),
        Short.MaxValue, precision)
        .map(hash => hash.substring(0, length))
        .filter(gh => broadcast.value._2.contains(gh))
        .foreach { h =>
          broadcast.value._2(h).foreach { i =>
            val lf = broadcast.value._1(i)
            val leftGeo = lf.getAttribute(leftIndex).asInstanceOf[Geometry]
            if (SpatialJoinUtils.relationJudge(isLeft, function, leftGeo, rightGeo)) {
              joins.append((lf, rf))
            }
          }
        }
      joins.iterator
    }
    SpatialJoinUtils.buildScan(leftRel, rightRel, joinedRows)
  }

  def distanceScan(sqlContext: SQLContext,
                   leftRel: GeoMesaRelation,
                   rightRel: GeoMesaRelation,
                   condition: Expression): RDD[Row] = {
    val (greatLeftI, greatRightI, greatEqual, greatDis, lessLeftI, lessRightI, lessEqual, lessDis) =
      SpatialJoinUtils.distanceExpress(leftRel, rightRel, condition)

    if (lessDis < greatDis) {
      sqlContext.sparkContext.emptyRDD[Row]
    } else {
      val precision = SpatialJoinUtils.getPrecision(lessDis)
      val length = precision / 5
      val features = leftRel.rawRDD.collect()
      val indices = features.indices.flatMap { i =>
        val geo = features(i).getAttribute(lessLeftI).asInstanceOf[Geometry]
        val env = RelationUtils.extractEnvelope(geo.getEnvelopeInternal, lessDis)
        BoundingBox.getGeoHashesFromBoundingBox(BoundingBox(env), Short.MaxValue, precision)
          .map(gh => (gh.substring(0, length), i))
      }.groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).toList))
      val broadcast = sqlContext.sparkContext.broadcast(
        (features.indices.map(i => (i, features(i))).toMap, indices)
      )

      val joinedRows = rightRel.rawRDD.flatMap { rf =>
        val joins = new ListBuffer[(SimpleFeature, SimpleFeature)]()
        val rightGeo = rf.getAttribute(lessRightI).asInstanceOf[Geometry]
        BoundingBox.getGeoHashesFromBoundingBox(
          BoundingBox(rightGeo.getEnvelopeInternal),
          Short.MaxValue, precision)
          .map(hash => hash.substring(0, length))
          .filter(gh => broadcast.value._2.contains(gh))
          .foreach { h =>
            broadcast.value._2(h).foreach { i =>
              val lf = broadcast.value._1(i)
              if (SpatialJoinUtils.distanceJudge(greatLeftI, greatRightI, greatEqual, greatDis,
                lessLeftI, lessRightI, lessEqual, lessDis, lf, rf)) {
                joins.append((lf, rf))
              }
            }
          }
        joins.iterator
      }
      SpatialJoinUtils.buildScan(leftRel, rightRel, joinedRows)
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[org.apache.spark.sql.sources.Filter]): RDD[Row] = {
    if (isDistance) {
      distanceScan(sqlContext, leftRel, rightRel, condition)
    } else {
      relationScan(sqlContext, leftRel, rightRel, condition)
    }
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter {
      case t@(_: IsNotNull | _: IsNull) => true
      case _ => false
    }
  }
}

class DistanceOverlapAction(greatLeftI: Int, greatRightI: Int,
                            greatEqual: Int, greatDis: Double,
                            lessLeftI: Int, lessRightI: Int,
                            lessEqual: Int, lessDis: Double) extends SweepLineOverlapAction with Serializable {

  val joins = ListBuffer[(SimpleFeature, SimpleFeature)]()

  override def overlap(s0: SweepLineInterval, s1: SweepLineInterval): Unit = {
    val (key0, lf) = s0.getItem.asInstanceOf[(Int, SimpleFeature)]
    val (key1, rf) = s1.getItem.asInstanceOf[(Int, SimpleFeature)]
    if (key0 == 0 && key1 == 1) {
      if (SpatialJoinUtils.distanceJudge(greatLeftI, greatRightI, greatEqual, greatDis,
        lessLeftI, lessRightI, lessEqual, lessDis, lf, rf)) {
        joins.append((lf, rf))
      }
    } else if (key0 == 1 && key1 == 0) {
      if (SpatialJoinUtils.distanceJudge(greatLeftI, greatRightI, greatEqual, greatDis,
        lessLeftI, lessRightI, lessEqual, lessDis, rf, lf)) {
        joins.append((rf, lf))
      }
    }
  }
}

object SpatialJoinUtils {
  val functionDistance = "st_distance"

  def getPrecision(buffer: Double = 0) = {
    if (buffer <= 0.02) 30
    else if (buffer <= 0.1) 25
    else if (buffer <= 1.0) 20
    else 15
  }

  def buildScan(leftRel: GeoMesaRelation,
                rightRel: GeoMesaRelation,
                joinedRows: RDD[(SimpleFeature, SimpleFeature)]): RDD[Row] = {
    val leftSchema = leftRel.schema
    val rightSchema = rightRel.schema
    val leftExtractors = SparkUtils.getExtractors(leftSchema.fieldNames, leftSchema)
    val rightExtractors = SparkUtils.getExtractors(rightSchema.fieldNames, rightSchema)

    joinedRows.mapPartitions { iter =>
      val joinedSchema = StructType(leftSchema.fields ++ rightSchema.fields)
      val joinedExtractors = leftExtractors ++ rightExtractors
      iter.map { case (leftFeature, rightFeature) =>
        SparkUtils.joinedSf2row(joinedSchema, leftFeature, rightFeature, joinedExtractors)
      }
    }
  }

  def geoHashPartitioning(rawRDD: RDD[SimpleFeature], precision: Int, geomOrdinal: Int): List[Envelope] = {
    rawRDD.flatMap { f =>
      val geo = f.getAttribute(geomOrdinal).asInstanceOf[Geometry]
      geo match {
        case point: Point =>
          if (RelationUtils.globalEnvelope.contains(geo.getCoordinate)) {
            List(GeoHash(point, precision).hash).iterator
          } else {
            List[String]().iterator
          }
        case _ =>
          val env = RelationUtils.extractEnvelope(geo.getEnvelopeInternal)
          BoundingBox.getGeoHashesFromBoundingBox(BoundingBox(env), Short.MaxValue, precision)
            .filter(s => GeoHash(s, precision).geom.intersects(geo)).iterator
      }
    }.distinct().map(s => GeoHash(s, precision).bbox.envelope).collect().toList
  }

  def circlePartitioning(rdd: RDD[SimpleFeature], buffer: Double, geomOrdinal: Int): List[Envelope] = {
    val precision = getPrecision(buffer)
    rdd.flatMap { f =>
      val geom = f.getAttribute(geomOrdinal).asInstanceOf[Geometry]
      val env = RelationUtils.extractEnvelope(geom.getEnvelopeInternal, buffer)
      BoundingBox.getGeoHashesFromBoundingBox(BoundingBox(env), Short.MaxValue, precision).iterator
    }.distinct().map(s => GeoHash(s, precision).bbox.envelope).collect().toList
  }

  def distanceJudge(greatLeftI: Int, greatRightI: Int,
                    greatEqual: Int, greatDis: Double,
                    lessLeftI: Int, lessRightI: Int,
                    lessEqual: Int, lessDis: Double,
                    left: SimpleFeature, right: SimpleFeature): Boolean = {
    if (greatEqual == -1) {
      val dis = left.getAttribute(lessLeftI).asInstanceOf[Geometry].distance(right.getAttribute(lessRightI).asInstanceOf[Geometry])
      lessEqual match {
        case 0 if dis <= lessDis => true
        case 1 if dis < lessDis => true
        case 2 if Math.abs(dis - lessDis) <= 1.0E-6 => true
        case _ => false
      }
    } else {
      val lessDist = left.getAttribute(lessLeftI).asInstanceOf[Geometry].distance(right.getAttribute(lessRightI).asInstanceOf[Geometry])
      val greatDist = if (greatLeftI == lessLeftI && greatRightI == lessRightI) {
        lessDist
      } else {
        left.getAttribute(greatLeftI).asInstanceOf[Geometry].distance(right.getAttribute(greatRightI).asInstanceOf[Geometry])
      }
      (greatEqual, lessEqual) match {
        case (0, 0) if lessDist <= lessDis && greatDist >= greatDis => true
        case (0, 1) if lessDist < lessDis && greatDist >= greatDis => true
        case (1, 0) if lessDist <= lessDis && greatDist > greatDis => true
        case (1, 1) if lessDist < lessDis && greatDist > greatDis => true
        case _ => false
      }
    }
  }

  def relationJudge(isLeft: Boolean, function: (Geometry, Geometry) => Boolean,
                    leftGeo: Geometry, rightGeo: Geometry): Boolean = {
    (isLeft && function(leftGeo, rightGeo)) || (!isLeft && function(rightGeo, leftGeo))
  }

  def relationExpress(leftRel: GeoMesaRelation,
                      rightRel: GeoMesaRelation,
                      condition: Expression): (Boolean, Int, Int, (Geometry, Geometry) => Boolean) = {
    val scalaUdf = condition.asInstanceOf[ScalaUDF]
    val function = scalaUdf.function.asInstanceOf[(Geometry, Geometry) => Boolean]
    val children = scalaUdf.children.asInstanceOf[Seq[AttributeReference]]
    val leftAttr = children.head.name
    val rightAttr = children(1).name
    val leftIndex = leftRel.sft.indexOf(leftAttr)
    if (leftIndex == -1) {
      (false, leftRel.sft.indexOf(rightAttr), rightRel.sft.indexOf(leftAttr), function)
    } else {
      (true, leftIndex, rightRel.sft.indexOf(rightAttr), function)
    }
  }

  def distanceExpress(leftRel: GeoMesaRelation,
                      rightRel: GeoMesaRelation,
                      condition: Expression): (Int, Int, Int, Double, Int, Int, Int, Double) = {
    condition match {
      case u: LessThanOrEqual =>
        val leftAttr = u.left.children.head.asInstanceOf[AttributeReference].name
        val rightAttr = u.left.children(1).asInstanceOf[AttributeReference].name
        val index = leftRel.sft.indexOf(leftAttr)
        val dis = u.right.asInstanceOf[Literal].value.asInstanceOf[Double]
        val (leftI, rightI) = if (index == -1) {
          (leftRel.sft.indexOf(rightAttr), rightRel.sft.indexOf(leftAttr))
        } else {
          (index, rightRel.sft.indexOf(rightAttr))
        }
        (leftI, rightI, -1, 0, leftI, rightI, 0, dis)
      case u: LessThan =>
        val leftAttr = u.left.children.head.asInstanceOf[AttributeReference].name
        val rightAttr = u.left.children(1).asInstanceOf[AttributeReference].name
        val index = leftRel.sft.indexOf(leftAttr)
        val dis = u.right.asInstanceOf[Literal].value.asInstanceOf[Double]
        val (leftI, rightI) = if (index == -1) {
          (leftRel.sft.indexOf(rightAttr), rightRel.sft.indexOf(leftAttr))
        } else {
          (index, rightRel.sft.indexOf(rightAttr))
        }
        (leftI, rightI, -1, 0, leftI, rightI, 1, dis)
      case u: EqualTo =>
        val leftAttr = u.left.children.head.asInstanceOf[AttributeReference].name
        val rightAttr = u.left.children(1).asInstanceOf[AttributeReference].name
        val index = leftRel.sft.indexOf(leftAttr)
        val dis = u.right.asInstanceOf[Literal].value.asInstanceOf[Double]
        val (leftI, rightI) = if (index == -1) {
          (leftRel.sft.indexOf(rightAttr), rightRel.sft.indexOf(leftAttr))
        } else {
          (index, rightRel.sft.indexOf(rightAttr))
        }
        (leftI, rightI, -1, 0, leftI, rightI, 2, dis)
      case u: And =>
        (u.left, u.right) match {
          case (lu: GreaterThanOrEqual, ru: LessThanOrEqual) =>
            val leftLAttr = lu.left.children.head.asInstanceOf[AttributeReference].name
            val leftRAttr = lu.left.children(1).asInstanceOf[AttributeReference].name
            val leftIndex = leftRel.sft.indexOf(leftLAttr)
            val leftDis = lu.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (greatLeftI, greatRightI) = if (leftIndex == -1) {
              (leftRel.sft.indexOf(leftRAttr), rightRel.sft.indexOf(leftLAttr))
            } else {
              (leftIndex, rightRel.sft.indexOf(leftRAttr))
            }
            val rightLAttr = ru.left.children.head.asInstanceOf[AttributeReference].name
            val rightRAttr = ru.left.children(1).asInstanceOf[AttributeReference].name
            val rightIndex = leftRel.sft.indexOf(rightLAttr)
            val rightDis = ru.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (lessLeftI, lessRightI) = if (rightIndex == -1) {
              (leftRel.sft.indexOf(rightRAttr), rightRel.sft.indexOf(rightLAttr))
            } else {
              (rightIndex, rightRel.sft.indexOf(rightRAttr))
            }
            (greatLeftI, greatRightI, 0, leftDis, lessLeftI, lessRightI, 0, rightDis)
          case (lu: GreaterThanOrEqual, ru: LessThan) =>
            val leftLAttr = lu.left.children.head.asInstanceOf[AttributeReference].name
            val leftRAttr = lu.left.children(1).asInstanceOf[AttributeReference].name
            val leftIndex = leftRel.sft.indexOf(leftLAttr)
            val leftDis = lu.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (greatLeftI, greatRightI) = if (leftIndex == -1) {
              (leftRel.sft.indexOf(leftRAttr), rightRel.sft.indexOf(leftLAttr))
            } else {
              (leftIndex, rightRel.sft.indexOf(leftRAttr))
            }
            val rightLAttr = ru.left.children.head.asInstanceOf[AttributeReference].name
            val rightRAttr = ru.left.children(1).asInstanceOf[AttributeReference].name
            val rightIndex = leftRel.sft.indexOf(rightLAttr)
            val rightDis = ru.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (lessLeftI, lessRightI) = if (rightIndex == -1) {
              (leftRel.sft.indexOf(rightRAttr), rightRel.sft.indexOf(rightLAttr))
            } else {
              (rightIndex, rightRel.sft.indexOf(rightRAttr))
            }
            (greatLeftI, greatRightI, 0, leftDis, lessLeftI, lessRightI, 1, rightDis)
          case (lu: GreaterThan, ru: LessThanOrEqual) =>
            val leftLAttr = lu.left.children.head.asInstanceOf[AttributeReference].name
            val leftRAttr = lu.left.children(1).asInstanceOf[AttributeReference].name
            val leftIndex = leftRel.sft.indexOf(leftLAttr)
            val leftDis = lu.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (greatLeftI, greatRightI) = if (leftIndex == -1) {
              (leftRel.sft.indexOf(leftRAttr), rightRel.sft.indexOf(leftLAttr))
            } else {
              (leftIndex, rightRel.sft.indexOf(leftRAttr))
            }
            val rightLAttr = ru.left.children.head.asInstanceOf[AttributeReference].name
            val rightRAttr = ru.left.children(1).asInstanceOf[AttributeReference].name
            val rightIndex = leftRel.sft.indexOf(rightLAttr)
            val rightDis = ru.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (lessLeftI, lessRightI) = if (rightIndex == -1) {
              (leftRel.sft.indexOf(rightRAttr), rightRel.sft.indexOf(rightLAttr))
            } else {
              (rightIndex, rightRel.sft.indexOf(rightRAttr))
            }
            (greatLeftI, greatRightI, 1, leftDis, lessLeftI, lessRightI, 0, rightDis)
          case (lu: GreaterThan, ru: LessThan) =>
            val leftLAttr = lu.left.children.head.asInstanceOf[AttributeReference].name
            val leftRAttr = lu.left.children(1).asInstanceOf[AttributeReference].name
            val leftIndex = leftRel.sft.indexOf(leftLAttr)
            val leftDis = lu.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (greatLeftI, greatRightI) = if (leftIndex == -1) {
              (leftRel.sft.indexOf(leftRAttr), rightRel.sft.indexOf(leftLAttr))
            } else {
              (leftIndex, rightRel.sft.indexOf(leftRAttr))
            }
            val rightLAttr = ru.left.children.head.asInstanceOf[AttributeReference].name
            val rightRAttr = ru.left.children(1).asInstanceOf[AttributeReference].name
            val rightIndex = leftRel.sft.indexOf(rightLAttr)
            val rightDis = ru.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (lessLeftI, lessRightI) = if (rightIndex == -1) {
              (leftRel.sft.indexOf(rightRAttr), rightRel.sft.indexOf(rightLAttr))
            } else {
              (rightIndex, rightRel.sft.indexOf(rightRAttr))
            }
            (greatLeftI, greatRightI, 1, leftDis, lessLeftI, lessRightI, 1, rightDis)

          case (lu: LessThanOrEqual, ru: GreaterThanOrEqual) =>
            val leftLAttr = ru.left.children.head.asInstanceOf[AttributeReference].name
            val leftRAttr = ru.left.children(1).asInstanceOf[AttributeReference].name
            val leftIndex = leftRel.sft.indexOf(leftLAttr)
            val leftDis = ru.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (greatLeftI, greatRightI) = if (leftIndex == -1) {
              (leftRel.sft.indexOf(leftRAttr), rightRel.sft.indexOf(leftLAttr))
            } else {
              (leftIndex, rightRel.sft.indexOf(leftRAttr))
            }
            val rightLAttr = lu.left.children.head.asInstanceOf[AttributeReference].name
            val rightRAttr = lu.left.children(1).asInstanceOf[AttributeReference].name
            val rightIndex = leftRel.sft.indexOf(rightLAttr)
            val rightDis = lu.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (lessLeftI, lessRightI) = if (rightIndex == -1) {
              (leftRel.sft.indexOf(rightRAttr), rightRel.sft.indexOf(rightLAttr))
            } else {
              (rightIndex, rightRel.sft.indexOf(rightRAttr))
            }
            (greatLeftI, greatRightI, 0, leftDis, lessLeftI, lessRightI, 0, rightDis)
          case (lu: LessThanOrEqual, ru: GreaterThan) =>
            val leftLAttr = ru.left.children.head.asInstanceOf[AttributeReference].name
            val leftRAttr = ru.left.children(1).asInstanceOf[AttributeReference].name
            val leftIndex = leftRel.sft.indexOf(leftLAttr)
            val leftDis = ru.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (greatLeftI, greatRightI) = if (leftIndex == -1) {
              (leftRel.sft.indexOf(leftRAttr), rightRel.sft.indexOf(leftLAttr))
            } else {
              (leftIndex, rightRel.sft.indexOf(leftRAttr))
            }
            val rightLAttr = lu.left.children.head.asInstanceOf[AttributeReference].name
            val rightRAttr = lu.left.children(1).asInstanceOf[AttributeReference].name
            val rightIndex = leftRel.sft.indexOf(rightLAttr)
            val rightDis = lu.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (lessLeftI, lessRightI) = if (rightIndex == -1) {
              (leftRel.sft.indexOf(rightRAttr), rightRel.sft.indexOf(rightLAttr))
            } else {
              (rightIndex, rightRel.sft.indexOf(rightRAttr))
            }
            (greatLeftI, greatRightI, 1, leftDis, lessLeftI, lessRightI, 0, rightDis)
          case (lu: LessThan, ru: GreaterThanOrEqual) =>
            val leftLAttr = ru.left.children.head.asInstanceOf[AttributeReference].name
            val leftRAttr = ru.left.children(1).asInstanceOf[AttributeReference].name
            val leftIndex = leftRel.sft.indexOf(leftLAttr)
            val leftDis = ru.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (greatLeftI, greatRightI) = if (leftIndex == -1) {
              (leftRel.sft.indexOf(leftRAttr), rightRel.sft.indexOf(leftLAttr))
            } else {
              (leftIndex, rightRel.sft.indexOf(leftRAttr))
            }
            val rightLAttr = lu.left.children.head.asInstanceOf[AttributeReference].name
            val rightRAttr = lu.left.children(1).asInstanceOf[AttributeReference].name
            val rightIndex = leftRel.sft.indexOf(rightLAttr)
            val rightDis = lu.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (lessLeftI, lessRightI) = if (rightIndex == -1) {
              (leftRel.sft.indexOf(rightRAttr), rightRel.sft.indexOf(rightLAttr))
            } else {
              (rightIndex, rightRel.sft.indexOf(rightRAttr))
            }
            (greatLeftI, greatRightI, 0, leftDis, lessLeftI, lessRightI, 1, rightDis)
          case (lu: LessThan, ru: GreaterThan) =>
            val leftLAttr = ru.left.children.head.asInstanceOf[AttributeReference].name
            val leftRAttr = ru.left.children(1).asInstanceOf[AttributeReference].name
            val leftIndex = leftRel.sft.indexOf(leftLAttr)
            val leftDis = ru.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (greatLeftI, greatRightI) = if (leftIndex == -1) {
              (leftRel.sft.indexOf(leftRAttr), rightRel.sft.indexOf(leftLAttr))
            } else {
              (leftIndex, rightRel.sft.indexOf(leftRAttr))
            }
            val rightLAttr = lu.left.children.head.asInstanceOf[AttributeReference].name
            val rightRAttr = lu.left.children(1).asInstanceOf[AttributeReference].name
            val rightIndex = leftRel.sft.indexOf(rightLAttr)
            val rightDis = lu.right.asInstanceOf[Literal].value.asInstanceOf[Double]
            val (lessLeftI, lessRightI) = if (rightIndex == -1) {
              (leftRel.sft.indexOf(rightRAttr), rightRel.sft.indexOf(rightLAttr))
            } else {
              (rightIndex, rightRel.sft.indexOf(rightRAttr))
            }
            (greatLeftI, greatRightI, 1, leftDis, lessLeftI, lessRightI, 1, rightDis)
          case _ => (-1, -1, -1, 0.0, -1, -1, -1, 0.0)
        }
      case _ => (-1, -1, -1, 0.0, -1, -1, -1, 0.0)
    }
  }

  def checkRangeDistance(join: Join): Boolean = {
    def checkDistance(u: ScalaUDF): Boolean = {
      u.udfName.exists(p => p.equalsIgnoreCase(SpatialJoinUtils.functionDistance)) &&
        u.children.head.isInstanceOf[AttributeReference] && u.children(1).isInstanceOf[AttributeReference]
    }

    def checkDistance2(lu: ScalaUDF, ru: ScalaUDF): Boolean = {
      lu.udfName.exists(p => p.equalsIgnoreCase(SpatialJoinUtils.functionDistance)) &&
        ru.udfName.exists(p => p.equalsIgnoreCase(SpatialJoinUtils.functionDistance)) &&
        lu.children.head.isInstanceOf[AttributeReference] && lu.children(1).isInstanceOf[AttributeReference] &&
        ru.children.head.isInstanceOf[AttributeReference] && ru.children(1).isInstanceOf[AttributeReference]
    }

    join.condition.exists {
      case u: LessThanOrEqual if u.left.isInstanceOf[ScalaUDF] => checkDistance(u.left.asInstanceOf[ScalaUDF])
      case u: LessThan if u.left.isInstanceOf[ScalaUDF] => checkDistance(u.left.asInstanceOf[ScalaUDF])
      case u: EqualTo if u.left.isInstanceOf[ScalaUDF] => checkDistance(u.left.asInstanceOf[ScalaUDF])
      case u: And =>
        (u.left, u.right) match {
          case (lu: GreaterThanOrEqual, ru: LessThanOrEqual) if lu.left.isInstanceOf[ScalaUDF] && ru.left.isInstanceOf[ScalaUDF] =>
            checkDistance2(lu.left.asInstanceOf[ScalaUDF], ru.left.asInstanceOf[ScalaUDF])
          case (lu: GreaterThanOrEqual, ru: LessThan) if lu.left.isInstanceOf[ScalaUDF] && ru.left.isInstanceOf[ScalaUDF] =>
            checkDistance2(lu.left.asInstanceOf[ScalaUDF], ru.left.asInstanceOf[ScalaUDF])
          case (lu: GreaterThan, ru: LessThanOrEqual) if lu.left.isInstanceOf[ScalaUDF] && ru.left.isInstanceOf[ScalaUDF] =>
            checkDistance2(lu.left.asInstanceOf[ScalaUDF], ru.left.asInstanceOf[ScalaUDF])
          case (lu: GreaterThan, ru: LessThan) if lu.left.isInstanceOf[ScalaUDF] && ru.left.isInstanceOf[ScalaUDF] =>
            checkDistance2(lu.left.asInstanceOf[ScalaUDF], ru.left.asInstanceOf[ScalaUDF])

          case (lu: LessThanOrEqual, ru: GreaterThanOrEqual) if lu.left.isInstanceOf[ScalaUDF] && ru.left.isInstanceOf[ScalaUDF] =>
            checkDistance2(lu.left.asInstanceOf[ScalaUDF], ru.left.asInstanceOf[ScalaUDF])
          case (lu: LessThanOrEqual, ru: GreaterThan) if lu.left.isInstanceOf[ScalaUDF] && ru.left.isInstanceOf[ScalaUDF] =>
            checkDistance2(lu.left.asInstanceOf[ScalaUDF], ru.left.asInstanceOf[ScalaUDF])
          case (lu: LessThan, ru: GreaterThanOrEqual) if lu.left.isInstanceOf[ScalaUDF] && ru.left.isInstanceOf[ScalaUDF] =>
            checkDistance2(lu.left.asInstanceOf[ScalaUDF], ru.left.asInstanceOf[ScalaUDF])
          case (lu: LessThan, ru: GreaterThan) if lu.left.isInstanceOf[ScalaUDF] && ru.left.isInstanceOf[ScalaUDF] =>
            checkDistance2(lu.left.asInstanceOf[ScalaUDF], ru.left.asInstanceOf[ScalaUDF])

          case _ => false
        }
      case _ => false
    }
  }

}
