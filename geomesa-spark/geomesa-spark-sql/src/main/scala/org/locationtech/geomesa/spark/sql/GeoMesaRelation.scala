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
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
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
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.sql

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.memory.cqengine.datastore.GeoCQEngineDataStore
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.locationtech.geomesa.spark.sql.GeoMesaRelation.{CachedRDD, IndexedRDD, PartitionedIndexedRDD, PartitionedRDD}
import org.locationtech.geomesa.spark.sql.GeoMesaSparkSQL.GEOMESA_SQL_FEATURE
import org.locationtech.geomesa.spark.{GeoMesaSpark, SpatialRDD}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.Envelope

import java.util.{Collections, Locale}
import scala.util.control.NonFatal

/**
  * The Spark Relation that builds the scan over the GeoMesa table
  *
  * @param sqlContext spark sql context
  * @param sft simple feature type associated with the rows in the relation
  * @param schema spark sql schema (must correspond to the sft)
  * @param params user parameters, generally for configured the underlying data store and/or caching/partitioning
  * @param filter a push-down geotools filter applied to the relation
  * @param cached an optional cached RDD, used to speed up queries when enabled
  * @param partitioned an optional spatially partitioned RDD, used to speed up spatial joins when enabled
  */
case class GeoMesaRelation(
    sqlContext: SQLContext,
    sft: SimpleFeatureType,
    schema: StructType,
    params: Map[String, String],
    filter: Option[org.geotools.api.filter.Filter],
    cached: Option[CachedRDD],
    partitioned: Option[PartitionedRDD]
  ) extends BaseRelation with PrunedFilteredScan with LazyLogging {

  import scala.collection.JavaConverters._

  /**
    * Attempts to do an optimized join between two relations.
    *
    * Currently this method uses grid partitioning on both relations so that the join comparisons
    * only need to be applied on each pair of partitions, instead of globally. This only works
    * if both relations have already been grid partitioned.
    *
    * @param other relation to join
    * @param condition join condition
    * @return an optimized join, if possible to do so
    */
  def join(other: GeoMesaRelation, condition: Expression): Option[GeoMesaJoinRelation] = {
    val opt = for { p <- partitioned; o <- other.partitioned } yield {
      val toJoin = if (p.envelopes == o.envelopes) {
        Some(other)
      } else if (p.cover) {
        val repartitioned: SpatialRDD = p.partitions match {
          case None => p.raw
          case Some(partitions) => SpatialRDD(p.raw.repartition(partitions), p.raw.schema)
        }
        val parallelism = p.partitions.getOrElse(sqlContext.sparkContext.defaultParallelism)
        val rdd = RelationUtils.grid(repartitioned, p.envelopes, parallelism)
        val partitioned = Some(PartitionedRDD(rdd, p.raw, p.envelopes, p.partitions, p.cover))
        Some(other.copy(partitioned = partitioned))
      } else {
        logger.warn("Joining across two relations that are not partitioned by the same scheme - unable to optimize")
        None
      }
      toJoin.map { rel =>
        GeoMesaJoinRelation(sqlContext, this, rel, StructType(schema.fields ++ rel.schema.fields), condition)
      }
    }
    opt.flatten
  }

  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[org.apache.spark.sql.sources.Filter]): RDD[Row] = {
    lazy val debug =
      s"filt = $filter, filters = ${filters.mkString(",")}, requiredColumns = ${requiredColumns.mkString(",")}"

    val filt = {
      val sum = Seq.newBuilder[org.geotools.api.filter.Filter]
      filter.foreach(sum += _)
      filters.foreach(f => SparkUtils.sparkFilterToCQLFilter(f).foreach(sum += _))
      FilterHelper.filterListAsAnd(sum.result).getOrElse(org.geotools.api.filter.Filter.INCLUDE)
    }
    val requiredAttributes = requiredColumns.filterNot(_ == "__fid__")

    // avoid closures on complex objects
    val schema = this.schema // note: referencing case class members evidently serializes the whole class??
    val typeName = sft.getTypeName

    val result: RDD[SimpleFeature] = cached match {
      case None =>
        logger.debug(s"Building scan, $debug")
        val conf = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
        val query = new Query(typeName, filt, requiredAttributes: _*)
        GeoMesaSpark(params.asJava).rdd(conf, sqlContext.sparkContext, params, query)

      case Some(IndexedRDD(rdd)) =>
        logger.debug(s"Building in-memory scan, $debug")
        val cql = ECQL.toCQL(filt)
        rdd.flatMap { engine =>
          val query = new Query(typeName, ECQL.toFilter(cql), requiredAttributes: _*)
          SelfClosingIterator(engine.getFeatureReader(query, Transaction.AUTO_COMMIT))
        }

      case Some(PartitionedIndexedRDD(rdd, _)) =>
        logger.debug(s"Building partitioned in-memory scan, $debug")
        val cql = ECQL.toCQL(filt)
        rdd.flatMap { case (_, engine) =>
          val query = new Query(typeName, ECQL.toFilter(cql), requiredAttributes: _*)
          SelfClosingIterator(engine.getFeatureReader(query, Transaction.AUTO_COMMIT))
        }
    }

    val extractors = SparkUtils.getExtractors(requiredColumns, schema)

    result.map(SparkUtils.sf2row(schema, _, extractors))
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter {
      case _ @ (_:IsNotNull | _:IsNull) => true
      case _ => false
    }
  }
}

object GeoMesaRelation extends LazyLogging {

  import scala.collection.JavaConverters._

  /**
    * Create a new relation based on the input parameters
    *
    * @param sqlContext sql context
    * @param params parameters
    * @return
    */
  def apply(sqlContext: SQLContext, params: Map[String, String]): GeoMesaRelation = {
    val name = params.getOrElse(GEOMESA_SQL_FEATURE,
      throw new IllegalArgumentException(s"Feature type must be specified with '$GEOMESA_SQL_FEATURE'"))
    val sft = GeoMesaSpark(params.asJava).sft(params, name).getOrElse {
      throw new IllegalArgumentException(s"Could not load feature type with name '$name'")
    }
    apply(sqlContext, params, SparkUtils.createStructType(sft), sft)
  }

  /**
    * Create a new relation based on the input parameters, with the given schema
    *
    * @param sqlContext sql context
    * @param params parameters
    * @param schema schema
    * @return
    */
  def apply(sqlContext: SQLContext, params: Map[String, String], schema: StructType): GeoMesaRelation = {
    val name = params.getOrElse(GEOMESA_SQL_FEATURE,
      throw new IllegalArgumentException(s"Feature type must be specified with '$GEOMESA_SQL_FEATURE'"))
    val sft = GeoMesaSpark(params.asJava).sft(params, name).getOrElse {
      throw new IllegalArgumentException(s"Could not load feature type with name '$name'")
    }
    apply(sqlContext, params, schema, sft)
  }

  /**
    * Create a new relation based on the input parameters, with the given schema and underlying feature type
    *
    * @param sqlContext sql context
    * @param params parameters
    * @param schema schema
    * @param sft simple feature type
    * @return
    */
  def apply(
      sqlContext: SQLContext,
      params: Map[String, String],
      schema: StructType,
      sft: SimpleFeatureType): GeoMesaRelation = {

    logger.trace(s"Creating GeoMesaRelation with sft: $sft")

    def get[T](key: String, transform: String => T, default: => T): T = {
      params.get(key) match {
        case None => default
        case Some(v) =>
          try { transform(v) } catch {
            case NonFatal(e) => logger.error(s"Error evaluating param '$key' with value '$v':", e); default
          }
      }
    }

    def rawRDD: SpatialRDD = {
      val query = new Query(sft.getTypeName, ECQL.toFilter(params.getOrElse("query", "INCLUDE")))
      GeoMesaSpark(params.asJava).rdd(new Configuration(), sqlContext.sparkContext, params, query)
    }

    val partitioned = if (!get[Boolean]("spatial", _.toBoolean, false)) { None } else {
      val raw = rawRDD
      val bounds: Envelope = params.get("bounds") match {
        case None => RelationUtils.getBound(raw)
        case Some(b) =>
          try { WKTUtils.read(b).getEnvelopeInternal } catch {
            case NonFatal(e) => throw new IllegalArgumentException(s"Error reading provided bounds '$b':", e)
          }
      }

      val partitions = Option(get[Int]("partitions", _.toInt, -1)).filter(_ > 0)
      val parallelism = partitions.getOrElse(sqlContext.sparkContext.defaultParallelism)
      // control partitioning strategies that require a sample of the data
      lazy val sampleSize = get[Int]("sampleSize", _.toInt, 100)
      lazy val threshold = get[Double]("threshold", _.toDouble, 0.3)

      val envelopes = params.getOrElse("strategy", "equal").toLowerCase(Locale.US) match {
        case "equal"    => RelationUtils.equalPartitioning(bounds, parallelism)
        case "earth"    => RelationUtils.wholeEarthPartitioning(parallelism)
        case "weighted" => RelationUtils.weightedPartitioning(raw, bounds, parallelism, sampleSize)
        case "rtree"    => RelationUtils.rtreePartitioning(raw, parallelism, sampleSize, threshold)
        case s => throw new IllegalArgumentException(s"Invalid partitioning strategy: $s")
      }

      val rdd = RelationUtils.grid(raw, envelopes, parallelism)
      rdd.persist(StorageLevel.MEMORY_ONLY)
      Some(PartitionedRDD(rdd, raw, envelopes, partitions, get[Boolean]("cover", _.toBoolean, false)))
    }

    val cached = if (!get[Boolean]("cache", _.toBoolean, false)) { None } else {
      val check = Collections.singletonMap[String, java.io.Serializable]("cqengine", "true")
      if (!DataStoreFinder.getAvailableDataStores.asScala.exists(_.canProcess(check))) {
        throw new IllegalArgumentException("Caching requires the GeoCQEngineDataStore to be available on the classpath")
      }

      // avoid closure on full sft
      val typeName = sft.getTypeName
      val encodedSft = SimpleFeatureTypes.encodeType(sft, includeUserData = true)

      val indexGeom = get[Boolean]("indexGeom", _.toBoolean, false)

      partitioned match {
        case Some(p) =>
          val rdd = p.rdd.mapValues { iter =>
            val engine = new GeoCQEngineDataStore(indexGeom)
            engine.createSchema(SimpleFeatureTypes.createType(typeName, encodedSft))
            engine.namesToEngine.get(typeName).insert(iter)
            engine
          }
          p.rdd.unpersist() // make this call blocking?
          rdd.persist(StorageLevel.MEMORY_ONLY)
          Some(PartitionedIndexedRDD(rdd, p.envelopes))

        case None =>
          val rdd = rawRDD.mapPartitions { iter =>
            val engine = new GeoCQEngineDataStore(indexGeom)
            engine.createSchema(SimpleFeatureTypes.createType(typeName, encodedSft))
            engine.namesToEngine.get(typeName).insert(iter.toList)
            Iterator.single(engine)
          }
          rdd.persist(StorageLevel.MEMORY_ONLY)
          Some(IndexedRDD(rdd))
      }
    }
   
   val filter = Option(ECQL.toFilter(params.getOrElse("query", "INCLUDE")))

    GeoMesaRelation(sqlContext, sft, schema, params, filter, cached, partitioned)
  }

  /**
    * Holder for a partitioning scheme
    *
    * @param rdd partitioned rdd
    * @param raw underlying unpartitioned rdd
    * @param envelopes envelopes used in partitioning
    * @param partitions hint for number of partitions
    * @param cover cover partitions or not when joining
    */
  case class PartitionedRDD(
      rdd: RDD[(Int, Iterable[SimpleFeature])],
      raw: SpatialRDD,
      envelopes: List[Envelope],
      partitions: Option[Int],
      cover: Boolean
    )

  /**
    * Trait for cached RDDs used to accelerate scans
    */
  sealed trait CachedRDD

  /**
    * An RDD where each element is a spatial index containing multiple features
    *
    * @param rdd indexed features
    */
  case class IndexedRDD(rdd: RDD[GeoCQEngineDataStore]) extends CachedRDD

  /**
    * An RDD where each element is a spatial index containing multiple features, partitioned by
    * a spatial grid
    *
    * @param rdd grid cell -> indexed features
    * @param envelopes envelopes corresponding the each grid cell
    */
  case class PartitionedIndexedRDD(rdd: RDD[(Int, GeoCQEngineDataStore)], envelopes: List[Envelope])
      extends CachedRDD
}
