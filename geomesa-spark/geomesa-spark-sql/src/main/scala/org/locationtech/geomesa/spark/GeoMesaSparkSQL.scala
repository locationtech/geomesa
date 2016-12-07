/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark

import java.sql.Timestamp
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql._
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.opengis.feature.`type`._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.Try

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
class GeoMesaDataSource extends DataSourceRegister with RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  override def shortName(): String = "geomesa"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    SQLTypes.init(sqlContext)
    val ds = DataStoreFinder.getDataStore(parameters)
    val sft = ds.getSchema(parameters("geomesa.feature"))
    val schema = sft2StructType(sft)
    GeoMesaRelation(sqlContext, sft, schema, parameters)
  }

  // JNH: Q: Why doesn't this method have the call to SQLTypes.init(sqlContext)?
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val ds = DataStoreFinder.getDataStore(parameters)
    val sft = ds.getSchema(parameters("geomesa.feature"))
    GeoMesaRelation(sqlContext, sft, schema, parameters)
  }

  private def sft2StructType(sft: SimpleFeatureType) = {
    val fields = sft.getAttributeDescriptors.map { ad => ad2field(ad) }.toList
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

          case SQLTypes.PointType => builder.add(field.name, classOf[com.vividsolutions.jts.geom.Point])
          case SQLTypes.LineStringType => builder.add(field.name, classOf[com.vividsolutions.jts.geom.LineString])
          case SQLTypes.PolygonType  => builder.add(field.name, classOf[com.vividsolutions.jts.geom.Polygon])
          case SQLTypes.MultipolygonType  => builder.add(field.name, classOf[com.vividsolutions.jts.geom.MultiPolygon])
          case SQLTypes.GeometryType => builder.add(field.name, classOf[com.vividsolutions.jts.geom.Geometry])
        }
    }
    builder.setName(name)
    builder.buildFeatureType()
  }

  private def ad2field(ad: AttributeDescriptor): StructField = {
    import java.{lang => jl}
    val dt = ad.getType.getBinding match {
      case t if t == classOf[jl.Double]                       => DataTypes.DoubleType
      case t if t == classOf[jl.Float]                        => DataTypes.FloatType
      case t if t == classOf[jl.Integer]                      => DataTypes.IntegerType
      case t if t == classOf[jl.String]                       => DataTypes.StringType
      case t if t == classOf[jl.Boolean]                      => DataTypes.BooleanType
      case t if t == classOf[jl.Long]                         => DataTypes.LongType
      case t if t == classOf[java.util.Date]                  => DataTypes.TimestampType

      case t if t == classOf[com.vividsolutions.jts.geom.Point]        => SQLTypes.PointType
      case t if t == classOf[com.vividsolutions.jts.geom.LineString]   => SQLTypes.LineStringType
      case t if t == classOf[com.vividsolutions.jts.geom.Polygon]      => SQLTypes.PolygonType
      case t if t == classOf[com.vividsolutions.jts.geom.MultiPolygon] => SQLTypes.MultipolygonType
      // JNH: Add Geometry types here.

      case t if      classOf[Geometry].isAssignableFrom(t)    => SQLTypes.GeometryType

      case _                                                  => null
    }
    StructField(ad.getLocalName, dt)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    println("In createRelation with map " + parameters)
    val newFeatureName: String = parameters("geomesa.feature")
    val sft: SimpleFeatureType = structType2SFT(data.schema, newFeatureName)

    val ds = DataStoreFinder.getDataStore(parameters)
    println(s"Creating SFT: $sft")
    sft.getUserData.put("override.reserved.words", java.lang.Boolean.TRUE)
    ds.createSchema(sft)

    val rddToSave: RDD[SimpleFeature] = data.rdd.mapPartitions( iterRow => {
      val innerDS = DataStoreFinder.getDataStore(parameters)
      val sft = innerDS.getSchema(newFeatureName)
      val func: (Row) => SimpleFeature = SparkUtils.row2Sf(sft, _)
      iterRow.map(func)
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
    SparkUtils.buildScan(sft, requiredColumns, filters, filt, sqlContext.sparkContext, schema, params)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter {
      case t @ (_:IsNotNull | _:IsNull) => true
      case _ => false
    }
  }
}

object SparkUtils {
  @transient val ff = CommonFactoryFinder.getFilterFactory2
  private val log = org.slf4j.LoggerFactory.getLogger("org.locationtech.geomesa.accumulo.spark.sql")

  def buildScan(sft: SimpleFeatureType,
                requiredColumns: Array[String],
                filters: Array[org.apache.spark.sql.sources.Filter],
                filt: org.opengis.filter.Filter,
                ctx: SparkContext,
                schema: StructType,
                params: Map[String, String]): RDD[Row] = {
    log.warn(s"""Building scan, filt = $filt, filters = ${filters.mkString(",")}, requiredColumns = ${requiredColumns.mkString(",")}""")
    val compiledCQL = filters.flatMap(sparkFilterToCQLFilter).foldLeft[org.opengis.filter.Filter](filt) { (l, r) => ff.and(l,r) }
    log.warn(s"compiledCQL = $compiledCQL")

    val requiredAttributes = requiredColumns.filterNot(_ == "__fid__")
    val rdd = GeoMesaSpark(params).rdd(
      new Configuration(), ctx, params,
      new Query(params("geomesa.feature"), compiledCQL, requiredAttributes),
      Try(params("useMock").toBoolean).getOrElse(false), Option.empty[Int])

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
    case EqualTo(attr, v)                 => Some(ff.equals(ff.property(attr), ff.literal(v)))
    case In(attr, values)                 => Some(values.map(v => ff.equals(ff.property(attr), ff.literal(v))).reduce[org.opengis.filter.Filter]( (l,r) => ff.or(l,r)))
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
  def row2Sf(sft: SimpleFeatureType, row: Row): SimpleFeature = {
    val builder = new SimpleFeatureBuilder(sft)

    sft.getAttributeDescriptors.foreach {
      ad =>
        val name = ad.getLocalName
        val binding = ad.getType.getBinding

        if (binding == classOf[java.lang.String]) {
          builder.set(name, row.getAs[String](name))
        } else if (binding == classOf[java.lang.Double]) {
          builder.set(name, row.getAs[Double](name))
        } else if (binding == classOf[com.vividsolutions.jts.geom.Point]) {
          builder.set(name, row.getAs[Point](name))
        } else {
          println(s"UNHANDLED BINDING: $binding")
        }
    }

//    val descriptorNames: scala.collection.mutable.Buffer[String] = sft.getAttributeDescriptors.map(_.getLocalName)
//
//    row.getValuesMap(descriptorNames).foreach {
//      case (field, value) =>
//        val f: String = field
//        val v: Any = value
//        builder.set(field, value)
//    }

//    val fid = row.getAs[String]("__fid__")
    builder.buildFeature(UUID.randomUUID().toString)
  }

}
