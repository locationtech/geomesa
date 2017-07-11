/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.sql.Timestamp
import java.time.Instant
import java.util.{Date, UUID}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.index.strtree.{AbstractNode, Boundable, STRtree}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType, TimestampType}
import org.apache.spark.storage.StorageLevel
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs, SimpleFeatureTypes}
import org.opengis.feature.`type`._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Try

object GeoMesaSparkSQL {
  val GEOMESA_SQL_FEATURE = "geomesa.feature"
}

import org.locationtech.geomesa.spark.GeoMesaSparkSQL._

// Spark DataSource for GeoMesa
// enables loading a GeoMesa DataFrame as
// {{
// val df = spark.read
//   .format("geomesa")
//   .option(GM.instanceIdParam.getName, "mycloud")
//   .option(GM.userParam.getName, "user")
//   .option(GM.passwordParam.getName, "password")
//   .option(GM.tableNameParam.getName, "sparksql")
//   .option(GM.mockParam.getName, "true")
//   .option("geomesa.feature", "chicago")
//   .load()
// }}
class GeoMesaDataSource extends DataSourceRegister
  with RelationProvider with SchemaRelationProvider with CreatableRelationProvider
  with LazyLogging {
  import CaseInsensitiveMapFix._

  override def shortName(): String = "geomesa"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    SQLTypes.init(sqlContext)

    // TODO: Need different ways to retrieve sft
    //  GEOMESA-1643 Add method to lookup SFT to RDD Provider
    //  Below the details of the Converter RDD Provider and Providers which are backed by GT DSes are leaking through
    val ds = DataStoreFinder.getDataStore(parameters)
    val sft = if (ds != null) {
      ds.getSchema(parameters(GEOMESA_SQL_FEATURE))
    } else {
      if (parameters.contains(GEOMESA_SQL_FEATURE) && parameters.contains("geomesa.sft")) {
        SimpleFeatureTypes.createType(parameters(GEOMESA_SQL_FEATURE), parameters("geomesa.sft"))
      } else {
        SftArgResolver.getArg(SftArgs(parameters(GEOMESA_SQL_FEATURE), parameters(GEOMESA_SQL_FEATURE))) match {
          case Right(s) => s
          case Left(e) => throw new IllegalArgumentException("Could not resolve simple feature type", e)
        }

      }
    }
    logger.trace(s"Creating GeoMesa Relation with sft : $sft")

    val schema = sft2StructType(sft)
    GeoMesaRelation(sqlContext, sft, schema, parameters)
  }

  // JNH: Q: Why doesn't this method have the call to SQLTypes.init(sqlContext)?
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val ds = DataStoreFinder.getDataStore(parameters)
    val sft = ds.getSchema(parameters(GEOMESA_SQL_FEATURE))
    GeoMesaRelation(sqlContext, sft, schema, parameters)
  }

  private def sft2StructType(sft: SimpleFeatureType) = {
    val fields = sft.getAttributeDescriptors.flatMap { ad => ad2field(ad) }.toList
    StructType(StructField("__fid__", DataTypes.StringType, nullable =false) :: fields)
  }

  def structType2SFT(struct: StructType, name: String): SimpleFeatureType = {
    import java.{lang => jl}
    val fields = struct.fields

    val builder = new SimpleFeatureTypeBuilder

    fields.filter( _.name != "__fid__").foreach {
      field =>
        field.dataType match {
          case DataTypes.BooleanType => builder.add(field.name, classOf[jl.Boolean])
          case DataTypes.DateType => builder.add(field.name, classOf[java.util.Date])
          case DataTypes.FloatType => builder.add(field.name, classOf[jl.Float])
          case DataTypes.IntegerType => builder.add(field.name, classOf[jl.Integer])
          case DataTypes.DoubleType => builder.add(field.name, classOf[jl.Double])
          case DataTypes.StringType => builder.add(field.name, classOf[jl.String])
          case DataTypes.LongType   => builder.add(field.name, classOf[jl.Long])
          case DataTypes.TimestampType => builder.add(field.name, classOf[java.util.Date])

          case SQLTypes.PointTypeInstance => builder.add(field.name, classOf[com.vividsolutions.jts.geom.Point])
          case SQLTypes.LineStringTypeInstance => builder.add(field.name, classOf[com.vividsolutions.jts.geom.LineString])
          case SQLTypes.PolygonTypeInstance  => builder.add(field.name, classOf[com.vividsolutions.jts.geom.Polygon])
          case SQLTypes.MultipolygonTypeInstance  => builder.add(field.name, classOf[com.vividsolutions.jts.geom.MultiPolygon])
          case SQLTypes.GeometryTypeInstance => builder.add(field.name, classOf[com.vividsolutions.jts.geom.Geometry])
        }
    }
    builder.setName(name)
    builder.buildFeatureType()
  }

  private def ad2field(ad: AttributeDescriptor): Option[StructField] = {
    import java.{lang => jl}
    val dt = ad.getType.getBinding match {
      case t if t == classOf[jl.Double]                       => DataTypes.DoubleType
      case t if t == classOf[jl.Float]                        => DataTypes.FloatType
      case t if t == classOf[jl.Integer]                      => DataTypes.IntegerType
      case t if t == classOf[jl.String]                       => DataTypes.StringType
      case t if t == classOf[jl.Boolean]                      => DataTypes.BooleanType
      case t if t == classOf[jl.Long]                         => DataTypes.LongType
      case t if t == classOf[java.util.Date]                  => DataTypes.TimestampType

      case t if t == classOf[com.vividsolutions.jts.geom.Point]            => SQLTypes.PointTypeInstance
      case t if t == classOf[com.vividsolutions.jts.geom.MultiPoint]       => SQLTypes.MultiPointTypeInstance
      case t if t == classOf[com.vividsolutions.jts.geom.LineString]       => SQLTypes.LineStringTypeInstance
      case t if t == classOf[com.vividsolutions.jts.geom.MultiLineString]  => SQLTypes.MultiLineStringTypeInstance
      case t if t == classOf[com.vividsolutions.jts.geom.Polygon]          => SQLTypes.PolygonTypeInstance
      case t if t == classOf[com.vividsolutions.jts.geom.MultiPolygon]     => SQLTypes.MultipolygonTypeInstance

      case t if      classOf[Geometry].isAssignableFrom(t)    => SQLTypes.GeometryTypeInstance

      // NB:  List and Map types are not supported.
      case _                                                  => null
    }
    Option(dt).map(StructField(ad.getLocalName, _))
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val newFeatureName = parameters(GEOMESA_SQL_FEATURE)
    val sft: SimpleFeatureType = structType2SFT(data.schema, newFeatureName)

    // reuse the __fid__ if available for joins,
    // otherwise use a random id prefixed with the current time
    val fidIndex = data.schema.fields.indexWhere(_.name == "__fid__")
    val fidFn: Row => String =
      if(fidIndex > -1) (row: Row) => row.getString(fidIndex)
      else _ => "%d%s".format(Instant.now().getEpochSecond, UUID.randomUUID().toString)


    val ds = DataStoreFinder.getDataStore(parameters)
    sft.getUserData.put("override.reserved.words", java.lang.Boolean.TRUE)
    ds.createSchema(sft)

    val structType = if (data.queryExecution == null) {
      sft2StructType(sft)
    } else {
      data.schema
    }

    val rddToSave: RDD[SimpleFeature] = data.rdd.mapPartitions( iterRow => {
      val innerDS = DataStoreFinder.getDataStore(parameters)
      val sft = innerDS.getSchema(newFeatureName)
      val builder = new SimpleFeatureBuilder(sft)

      val nameMappings: List[(String, Int)] = SparkUtils.getSftRowNameMappings(sft, structType)
      iterRow.map { r =>
        SparkUtils.row2Sf(nameMappings, r, builder, fidFn(r))
      }
    })

    GeoMesaSpark(parameters).save(rddToSave, parameters, newFeatureName)

    GeoMesaRelation(sqlContext, sft, data.schema, parameters)
  }
}

// the Spark Relation that builds the scan over the GeoMesa table
// used by the SQL optimization rules to push spatio-temporal predicates into the `filt` variable
case class GeoMesaRelation(sqlContext: SQLContext,
                           sft: SimpleFeatureType,
                           schema: StructType,
                           params: Map[String, String],
                           filt: org.opengis.filter.Filter = org.opengis.filter.Filter.INCLUDE,
                           props: Option[Seq[String]] = None,
                           var indexRDD: RDD[GeoCQEngine] = null,
                           var partitionedRDD: RDD[(Int,Iterable[SimpleFeature])] = null,
                           var indexPartRDD: RDD[(Int, GeoCQEngine)] = null)
  extends BaseRelation with PrunedFilteredScan {

  lazy val isMock = Try(params("useMock").toBoolean).getOrElse(false)

  val cache = Try(params("cache").toBoolean).getOrElse(false)

  val indexId = Try(params("indexId").toBoolean).getOrElse(false)

  val indexGeom = Try(params("indexGeom").toBoolean).getOrElse(false)

  val numPartitions = Try(params("partitions").toInt).getOrElse(4)

  val spatiallyPartition = Try(params("spatial").toBoolean).getOrElse(false)

  val partitionStrategy = Try(params("strategy").toString).getOrElse("EQUAL")

  // Control partitioning strategies that require a sample of the data
  val sampleSize = Try(params("sampleSize").toInt).getOrElse(100)
  val thresholdMultiplier = Try(params("threshold").toDouble).getOrElse(0.3)

  lazy val rawRDD: SpatialRDD = buildRawRDD

  def buildRawRDD = {
    val raw = GeoMesaSpark(params).rdd(
      new Configuration(), sqlContext.sparkContext, params,
      new Query(params(GEOMESA_SQL_FEATURE)))

    if (params.contains("partitions")) {
      SpatialRDD(raw.repartition(numPartitions), raw.schema)
    } else {
      raw
    }
  }

  val encodedSFT: String = org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.encodeType(sft, true)

  if (partitionedRDD == null && spatiallyPartition) {
    val bounds = SparkUtils.getBound(rawRDD)
    val partitionEnvelopes = partitionStrategy match {
      case "EQUAL" => SparkUtils.equalPartitioning(bounds, numPartitions)
      case "RTREE" => SparkUtils.rtreePartitioning(rawRDD, numPartitions, sampleSize, thresholdMultiplier)
    }


    // Print in WKT for QGIS debugging
    var i = 0
    partitionEnvelopes.foreach { env =>
      println(s"$i:POLYGON((${env.getMinX} ${env.getMinY}, ${env.getMinX} ${env.getMaxY}, " +
        s"${env.getMaxX} ${env.getMaxY}, ${env.getMaxX} ${env.getMinY},${env.getMinX} ${env.getMinY}))")
      i+=1
    }

    partitionedRDD = SparkUtils.spatiallyPartition(partitionEnvelopes, rawRDD, numPartitions)
    partitionedRDD.persist(StorageLevel.MEMORY_ONLY)
  }

  if (indexRDD == null && indexPartRDD == null && cache) {
    if (spatiallyPartition) {
      indexPartRDD = SparkUtils.indexPartitioned(encodedSFT, partitionedRDD, indexId, indexGeom)
      partitionedRDD.unpersist() // make this call blocking?
      indexPartRDD.persist(StorageLevel.MEMORY_ONLY)
    } else {
      indexRDD = SparkUtils.index(encodedSFT, rawRDD, indexId, indexGeom)
      indexRDD.persist(StorageLevel.MEMORY_ONLY)
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[org.apache.spark.sql.sources.Filter]): RDD[Row] = {
    if (cache) {
      if (spatiallyPartition) {
        SparkUtils.buildScanInMemoryPartScan(requiredColumns, filters, filt, sqlContext.sparkContext, schema, params, indexPartRDD)
      } else {
        SparkUtils.buildScanInMemoryScan(requiredColumns, filters, filt, sqlContext.sparkContext, schema, params, indexRDD)
      }
    } else {
      SparkUtils.buildScan(requiredColumns, filters, filt, sqlContext.sparkContext, schema, params)
    }
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter {
      case t @ (_:IsNotNull | _:IsNull) => true
      case _ => false
    }
  }
}

object SparkUtils extends LazyLogging {
  import CaseInsensitiveMapFix._

  def indexIterator(encodedSft: String, indexId: Boolean, indexGeom: Boolean): GeoCQEngine = {
    val sft = org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.createType("", encodedSft)
    new org.locationtech.geomesa.memory.cqengine.GeoCQEngine(sft, indexId, indexGeom)
  }

  def index(encodedSft: String, rdd: RDD[SimpleFeature], indexId: Boolean, indexGeom: Boolean): RDD[GeoCQEngine] = {
    rdd.mapPartitions {
      iter =>
        val engine = SparkUtils.indexIterator(encodedSft, indexId, indexGeom)
        engine.addAll(iter.toList)
        Iterator(engine)
    }
  }

  def indexPartitioned(encodedSft: String,
                       rdd: RDD[(Int, Iterable[SimpleFeature])],
                       indexId: Boolean,
                       indexGeom: Boolean): RDD[(Int, GeoCQEngine)] = {

    rdd.mapValues { iter =>
        val engine = SparkUtils.indexIterator(encodedSft, indexId, indexGeom)
        engine.addAll(iter)
        engine
    }
  }

  // Maps a SimpleFeature to the id of the envelope that contains it
  // Will duplicate features that belong to more than one envelope
  // Returns -1 if no match was found
  // TODO: Filter duplicates when querying
  def gridIdMapper(sf: SimpleFeature, envelopes: List[Envelope]): List[(Int, SimpleFeature)] = {
    var result = new ListBuffer[(Int, SimpleFeature)]
    val geom = sf.getDefaultGeometry.asInstanceOf[Geometry]
    envelopes.indices.foreach { index =>
      if (envelopes(index).contains(geom.getEnvelopeInternal)) {
        val pair = (index, sf)
        result += pair
      }
    }
    if (result.isEmpty) {
      val pair = (-1, sf)
      result += pair
    }
    result.toList
  }

  def spatiallyPartition(envelopes: List[Envelope], rdd: RDD[SimpleFeature], numPartitions: Int): RDD[(Int, Iterable[SimpleFeature])] = {
    val keyedRdd = rdd.flatMap { gridIdMapper( _, envelopes)}
    val partitioned = keyedRdd.groupByKey(new HashPartitioner(numPartitions))
    partitioned.foreachPartition{ iter =>
      iter.foreach{ case (key, features) =>
        println(s"partition $key has ${features.size} features")
      }
    }
    partitioned
  }

  def getBound(rdd: RDD[SimpleFeature]): Envelope = {
    rdd.aggregate[Envelope](new Envelope())(
      (env: Envelope, sf: SimpleFeature) => {
        env.expandToInclude(sf.getDefaultGeometry.asInstanceOf[Geometry].getCoordinate)
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

  // Constructs an RTree based on a sample of the data and returns its bounds as envelopes
  // returns one less envelope than requested to account for the catch-all envelope
  def rtreePartitioning(rawRDD: RDD[SimpleFeature], numPartitions: Int, sampleSize: Int, thresholdMultiplier: Double): List[Envelope] = {
    val sample = rawRDD.takeSample(withReplacement = false, sampleSize)
    val rtree = new STRtree()

    sample.foreach{ sf =>
      rtree.insert(sf.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal, sf)
    }
    val envelopes: java.util.List[Envelope] = new java.util.ArrayList[Envelope]()

    // get rtree envelopes, limited to those containing reasonable size
    val reasonableSize = sampleSize / numPartitions
    val threshold = (reasonableSize*thresholdMultiplier).toInt
    val minSize = reasonableSize - threshold
    val maxSize = reasonableSize + threshold
    rtree.build()
    queryBoundary(rtree.getRoot, envelopes, minSize, maxSize)

    // check if any envelopes intersect
    envelopes.foreach { env1 =>
      envelopes.foreach {env2 =>
        if (env1.intersects(env2) && !env1.equals(env2)) {
          println (s"rtree envelopes $env1 and $env2 intersect")
        }
      }
    }
    envelopes.take(numPartitions-1).toList
  }

  // Helper method to get the envelopes of an RTree
  def queryBoundary(node: AbstractNode, boundaries: java.util.List[Envelope], minSize: Int, maxSize: Int): Int =  {
    // get node's immediate children
    val childBoundables: java.util.List[_] = node.getChildBoundables

    // True if current node is leaf
    var flagLeafnode = true
    var i = 0
    while ( { i < childBoundables.size && flagLeafnode}) {
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
          case (child: AbstractNode) =>
            val childSize = queryBoundary(child, boundaries, minSize, maxSize)
            // check boundary for size and existence in chosen boundaries
            if (childSize < maxSize && childSize > minSize) {
              var alreadyAdded = false
              if (node.getLevel != 1) {
                child.getChildBoundables.asInstanceOf[java.util.List[AbstractNode]].foreach { c =>
                  alreadyAdded = alreadyAdded || boundaries.contains(c.getBounds.asInstanceOf[Envelope])
                }
              }
              if (!alreadyAdded) {
                boundaries.add(child.getBounds.asInstanceOf[Envelope])
              }
            }
            nodeCount += childSize
          case (_) => nodeCount += 1 // negligible difference but accurate
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


  @transient val ff = CommonFactoryFinder.getFilterFactory2

  // the SFT attributes do not have the __fid__ so we have to translate accordingly
  def getExtractors(requiredColumns: Array[String], schema: StructType): Array[SimpleFeature => AnyRef] = {
    val requiredAttributes = requiredColumns.filterNot(_ == "__fid__")

    type EXTRACTOR = SimpleFeature => AnyRef
    val IdExtractor: SimpleFeature => AnyRef = sf => sf.getID

    requiredColumns.map {
      case "__fid__" => IdExtractor
      case col       =>
        val index = requiredAttributes.indexOf(col)
        val schemaIndex = schema.fieldIndex(col)
        val fieldType = schema.fields(schemaIndex).dataType
        sf: SimpleFeature =>
          if ( fieldType == TimestampType ) {
            new Timestamp(sf.getAttribute(index).asInstanceOf[Date].getTime)
          } else {
            sf.getAttribute(index)
          }
    }
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
    val compiledCQL = filters.flatMap(sparkFilterToCQLFilter).foldLeft[org.opengis.filter.Filter](filt) { (l, r) => ff.and(l,r) }
    logger.debug(s"compiledCQL = $compiledCQL")

    val requiredAttributes = requiredColumns.filterNot(_ == "__fid__")
    val rdd = GeoMesaSpark(params).rdd(
      new Configuration(), ctx, params,
      new Query(params(GEOMESA_SQL_FEATURE), compiledCQL, requiredAttributes))

    val extractors = getExtractors(requiredColumns, schema)
    val result = rdd.map(SparkUtils.sf2row(schema, _, extractors))
    result.asInstanceOf[RDD[Row]]
  }

  def buildScanInMemoryScan(requiredColumns: Array[String],
                filters: Array[org.apache.spark.sql.sources.Filter],
                filt: org.opengis.filter.Filter,
                ctx: SparkContext,
                schema: StructType,
                params: Map[String, String],  indexRDD: RDD[GeoCQEngine]): RDD[Row] = {

    logger.debug(
      s"""Building in-memory scan, filt = $filt,
         |filters = ${filters.mkString(",")},
         |requiredColumns = ${requiredColumns.mkString(",")}""".stripMargin)
    val compiledCQL = filters.flatMap(SparkUtils.sparkFilterToCQLFilter).foldLeft[org.opengis.filter.Filter](filt) { (l, r) => SparkUtils.ff.and(l,r) }
    logger.debug(s"compiledCQL = $compiledCQL")
    val filterString = ECQL.toCQL(compiledCQL)
    logger.debug(s"filterString = $filterString")


    val extractors = getExtractors(requiredColumns, schema)

    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeatureReader

    val result = indexRDD.flatMap { engine =>
      val filter = ECQL.toFilter(filterString)
      engine.queryCQ(filter).toIterator

    }.map(SparkUtils.sf2row(schema, _, extractors))

    result.asInstanceOf[RDD[Row]]
  }
  def buildScanInMemoryPartScan(requiredColumns: Array[String],
                                filters: Array[org.apache.spark.sql.sources.Filter],
                                filt: org.opengis.filter.Filter,
                                ctx: SparkContext,
                                schema: StructType,
                                params: Map[String, String],  indexPartRDD: RDD[(Int, GeoCQEngine)]): RDD[Row] = {

    logger.debug(
      s"""Building partitioned in-memory scan, filt = $filt,
         |filters = ${filters.mkString(",")},
         |requiredColumns = ${requiredColumns.mkString(",")}""".stripMargin)
    val compiledCQL = filters.flatMap(SparkUtils.sparkFilterToCQLFilter).foldLeft[org.opengis.filter.Filter](filt) { (l, r) => SparkUtils.ff.and(l,r) }
    logger.debug(s"compiledCQL = $compiledCQL")
    val filterString = ECQL.toCQL(compiledCQL)
    logger.debug(s"filterString = $filterString")


    // TODO: Could derive key from query and go straight to that partition
    // TODO: Could keep track of which keys are empty partitions and skip those

    val extractors = getExtractors(requiredColumns, schema)

    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeatureReader

    val result = indexPartRDD.flatMap { case (key, engine) =>
      val filter = ECQL.toFilter(filterString)
      engine.queryCQ(filter).toIterator
    }.map(SparkUtils.sf2row(schema, _, extractors))

    result.asInstanceOf[RDD[Row]]
  }


  def sparkFilterToCQLFilter(filt: org.apache.spark.sql.sources.Filter): Option[org.opengis.filter.Filter] = filt match {
    case GreaterThanOrEqual(attribute, v) => Some(ff.greaterOrEqual(ff.property(attribute), ff.literal(v)))
    case GreaterThan(attr, v)             => Some(ff.greater(ff.property(attr), ff.literal(v)))
    case LessThanOrEqual(attr, v)         => Some(ff.lessOrEqual(ff.property(attr), ff.literal(v)))
    case LessThan(attr, v)                => Some(ff.less(ff.property(attr), ff.literal(v)))
    case EqualTo(attr, v) if attr == "__fid__" => Some(ff.id(ff.featureId(v.toString)))
    case EqualTo(attr, v)                      => Some(ff.equals(ff.property(attr), ff.literal(v)))
    case In(attr, values) if attr == "__fid__" => Some(ff.id(values.map(v => ff.featureId(v.toString)).toSet))
    case In(attr, values)                      =>
      Some(values.map(v => ff.equals(ff.property(attr), ff.literal(v))).reduce[org.opengis.filter.Filter]( (l,r) => ff.or(l,r)))
    case And(left, right)                 => Some(ff.and(sparkFilterToCQLFilter(left).get, sparkFilterToCQLFilter(right).get)) // TODO: can these be null
    case Or(left, right)                  => Some(ff.or(sparkFilterToCQLFilter(left).get, sparkFilterToCQLFilter(right).get))
    case Not(f)                           => Some(ff.not(sparkFilterToCQLFilter(f).get))
    case StringStartsWith(a, v)           => Some(ff.like(ff.property(a), s"$v%"))
    case StringEndsWith(a, v)             => Some(ff.like(ff.property(a), s"%$v"))
    case StringContains(a, v)             => Some(ff.like(ff.property(a), s"%$v%"))
    case IsNull(attr)                     => None
    case IsNotNull(attr)                  => None
  }

  def sf2row(schema: StructType, sf: SimpleFeature, extractors: Array[SimpleFeature => AnyRef]): Row = {
    val res = Array.ofDim[Any](extractors.length)
    var i = 0
    while(i < extractors.length) {
      res(i) = extractors(i)(sf)
      i += 1
    }
    new GenericRowWithSchema(res, schema)
  }

  // Since each attribute's corresponding index in the Row is fixed. Compute the mapping once
  def getSftRowNameMappings(sft: SimpleFeatureType, schema: StructType): List[(String, Int)] = {
    sft.getAttributeDescriptors.map{ ad =>
      val name = ad.getLocalName
      (name, schema.fieldIndex(ad.getLocalName))
    }.toList
  }

  def row2Sf(nameMappings: List[(String, Int)], row: Row, builder: SimpleFeatureBuilder, id: String): SimpleFeature = {
    builder.reset()
    nameMappings.foreach{ case (name, index) =>
      builder.set(name, row.getAs[Object](index))
    }

    builder.userData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    builder.buildFeature(id)
  }
}
