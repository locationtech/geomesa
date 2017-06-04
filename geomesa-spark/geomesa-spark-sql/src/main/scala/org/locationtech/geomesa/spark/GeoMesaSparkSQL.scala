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
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs, SimpleFeatureTypes}
import org.opengis.feature.`type`._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.Try

object GeoMesaSparkSQL {
  val GEOMESA_SQL_FEATURE = "geomesa.feature"
}

import GeoMesaSparkSQL._

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

    val rddToSave: RDD[SimpleFeature] = data.rdd.mapPartitions( iterRow => {
      val innerDS = DataStoreFinder.getDataStore(parameters)
      val sft = innerDS.getSchema(newFeatureName)
      val builder = new SimpleFeatureBuilder(sft)

      iterRow.map { r =>
        builder.reset()
        SparkUtils.row2Sf(sft, r, builder, fidFn(r))
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
                           props: Option[Seq[String]] = None)
  extends BaseRelation with PrunedFilteredScan {

  lazy val isMock = Try(params("useMock").toBoolean).getOrElse(false)

  override def buildScan(requiredColumns: Array[String], filters: Array[org.apache.spark.sql.sources.Filter]): RDD[Row] = {
    SparkUtils.buildScan(requiredColumns, filters, filt, sqlContext.sparkContext, schema, params)
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

  @transient val ff = CommonFactoryFinder.getFilterFactory2

  def buildScan(requiredColumns: Array[String],
                filters: Array[org.apache.spark.sql.sources.Filter],
                filt: org.opengis.filter.Filter,
                ctx: SparkContext,
                schema: StructType,
                params: Map[String, String]): RDD[Row] = {
    logger.debug(s"""Building scan, filt = $filt, filters = ${filters.mkString(",")}, requiredColumns = ${requiredColumns.mkString(",")}""")
    val compiledCQL = filters.flatMap(sparkFilterToCQLFilter).foldLeft[org.opengis.filter.Filter](filt) { (l, r) => ff.and(l,r) }
    logger.debug(s"compiledCQL = $compiledCQL")

    val requiredAttributes = requiredColumns.filterNot(_ == "__fid__")
    val rdd = GeoMesaSpark(params).rdd(
      new Configuration(), ctx, params,
      new Query(params(GEOMESA_SQL_FEATURE), compiledCQL, requiredAttributes))

    type EXTRACTOR = SimpleFeature => AnyRef
    val IdExtractor: SimpleFeature => AnyRef = sf => sf.getID

    // the SFT attributes do not have the __fid__ so we have to translate accordingly
    val extractors: Array[EXTRACTOR] = requiredColumns.map {
      case "__fid__" => IdExtractor
      case col       =>
        val index = requiredAttributes.indexOf(col)
        sf: SimpleFeature => toSparkType(sf.getAttribute(index))
    }

    val result = rdd.map(SparkUtils.sf2row(schema, _, extractors))
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

  // TODO: optimize so we're not type checking every value
  def toSparkType(v: Any): AnyRef = v match {
    case t: Date => new Timestamp(t.getTime)
    case t       => t.asInstanceOf[AnyRef]
  }

  // JNH: Fix this.  Seriously.
  def row2Sf(sft: SimpleFeatureType, row: Row, builder: SimpleFeatureBuilder, id: String): SimpleFeature = {
    import java.{lang => jl}
    sft.getAttributeDescriptors.foreach {
      ad =>
        val name = ad.getLocalName
        // do we need to do type mapping here?
        val value = ad.getType.getBinding match {
          case t if t == classOf[jl.Double]                       => row.getAs[jl.Double](name)
          case t if t == classOf[jl.Float]                        => row.getAs[jl.Float](name)
          case t if t == classOf[jl.Integer]                      => row.getAs[jl.Integer](name)
          case t if t == classOf[jl.String]                       => row.getAs[jl.String](name)
          case t if t == classOf[jl.Boolean]                      => row.getAs[jl.Boolean](name)
          case t if t == classOf[jl.Long]                         => row.getAs[jl.Long](name)
          case t if t == classOf[java.util.Date]                  => row.getAs[java.sql.Timestamp](name) //timestamp extends date
          case t if t == classOf[com.vividsolutions.jts.geom.Point]            => row.getAs[Point](name)
          case t if t == classOf[com.vividsolutions.jts.geom.MultiPoint]       => row.getAs[MultiPoint](name)
          case t if t == classOf[com.vividsolutions.jts.geom.LineString]       => row.getAs[LineString](name)
          case t if t == classOf[com.vividsolutions.jts.geom.MultiLineString]  => row.getAs[MultiLineString](name)
          case t if t == classOf[com.vividsolutions.jts.geom.Polygon]          => row.getAs[Polygon](name)
          case t if t == classOf[com.vividsolutions.jts.geom.MultiPolygon]     => row.getAs[MultiPolygon](name)
          // JNH: Add Geometry types here.

          case t if      classOf[Geometry].isAssignableFrom(t)    => SQLTypes.GeometryTypeInstance
          case _                                                  => null
        }
        builder.set(name, value)
    }

    builder.userData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    builder.buildFeature(id)
  }
}
