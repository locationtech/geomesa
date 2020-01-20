/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.sql.Timestamp
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType, TimestampType}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.uuid.TimeSortedUuidGenerator
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.FilterFactory2

import scala.util.Try

object SparkUtils extends LazyLogging {

  import scala.collection.JavaConverters._

  @transient
  val ff: FilterFactory2 = CommonFactoryFinder.getFilterFactory2

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
        if (fieldType == TimestampType) {
          sf: SimpleFeature => {
            val attr = sf.getAttribute(index)
            if (attr == null) { null } else {
              new Timestamp(attr.asInstanceOf[Date].getTime)
            }
          }
        } else {
          sf: SimpleFeature => sf.getAttribute(index)
        }
    }
  }


  def sparkFilterToCQLFilter(filt: org.apache.spark.sql.sources.Filter): Option[org.opengis.filter.Filter] = filt match {
    case GreaterThanOrEqual(attribute, v) => Some(ff.greaterOrEqual(ff.property(attribute), ff.literal(v)))
    case GreaterThan(attr, v)             => Some(ff.greater(ff.property(attr), ff.literal(v)))
    case LessThanOrEqual(attr, v)         => Some(ff.lessOrEqual(ff.property(attr), ff.literal(v)))
    case LessThan(attr, v)                => Some(ff.less(ff.property(attr), ff.literal(v)))
    case EqualTo(attr, v) if attr == "__fid__" => Some(ff.id(ff.featureId(v.toString)))
    case EqualTo(attr, v)                      => Some(ff.equals(ff.property(attr), ff.literal(v)))
    case In(attr, values) if attr == "__fid__" => Some(ff.id(values.map(v => ff.featureId(v.toString)).toSet.asJava))
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

  def createFeatureType(name: String, struct: StructType): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder
    builder.setName(name)

    struct.fields.filter( _.name != "__fid__").foreach { field =>
      val binding = field.dataType match {
        case DataTypes.StringType              => classOf[java.lang.String]
        case DataTypes.DateType                => classOf[java.util.Date]
        case DataTypes.TimestampType           => classOf[java.util.Date]
        case DataTypes.IntegerType             => classOf[java.lang.Integer]
        case DataTypes.LongType                => classOf[java.lang.Long]
        case DataTypes.FloatType               => classOf[java.lang.Float]
        case DataTypes.DoubleType              => classOf[java.lang.Double]
        case DataTypes.BooleanType             => classOf[java.lang.Boolean]
        case JTSTypes.PointTypeInstance        => classOf[org.locationtech.jts.geom.Point]
        case JTSTypes.LineStringTypeInstance   => classOf[org.locationtech.jts.geom.LineString]
        case JTSTypes.PolygonTypeInstance      => classOf[org.locationtech.jts.geom.Polygon]
        case JTSTypes.MultipolygonTypeInstance => classOf[org.locationtech.jts.geom.MultiPolygon]
        case JTSTypes.GeometryTypeInstance     => classOf[org.locationtech.jts.geom.Geometry]
      }
      builder.add(field.name, binding)
    }

    builder.buildFeatureType()
  }

  def createStructType(sft: SimpleFeatureType): StructType = {
    val fields = sft.getAttributeDescriptors.asScala.flatMap(createStructField).toList
    StructType(StructField("__fid__", DataTypes.StringType, nullable = false) :: fields)
  }

  private def createStructField(ad: AttributeDescriptor): Option[StructField] = {
    val bindings = Try(ObjectType.selectType(ad)).getOrElse(Seq.empty)
    val dt = bindings.head match {
      case ObjectType.STRING   => DataTypes.StringType
      case ObjectType.INT      => DataTypes.IntegerType
      case ObjectType.LONG     => DataTypes.LongType
      case ObjectType.FLOAT    => DataTypes.FloatType
      case ObjectType.DOUBLE   => DataTypes.DoubleType
      case ObjectType.BOOLEAN  => DataTypes.BooleanType
      case ObjectType.DATE     => DataTypes.TimestampType
      case ObjectType.UUID     => null // not supported
      case ObjectType.BYTES    => null // not supported
      case ObjectType.LIST     => null // not supported
      case ObjectType.MAP      => null // not supported
      case ObjectType.GEOMETRY =>
        bindings.last match {
          case ObjectType.POINT               => JTSTypes.PointTypeInstance
          case ObjectType.LINESTRING          => JTSTypes.LineStringTypeInstance
          case ObjectType.POLYGON             => JTSTypes.PolygonTypeInstance
          case ObjectType.MULTIPOINT          => JTSTypes.MultiPointTypeInstance
          case ObjectType.MULTILINESTRING     => JTSTypes.MultiLineStringTypeInstance
          case ObjectType.MULTIPOLYGON        => JTSTypes.MultipolygonTypeInstance
          case ObjectType.GEOMETRY_COLLECTION => JTSTypes.GeometryTypeInstance
          case _                              => JTSTypes.GeometryTypeInstance
        }

      case _ => logger.warn(s"Unexpected bindings for descriptor $ad: ${bindings.mkString(", ")}"); null
    }
    Option(dt).map(StructField(ad.getLocalName, _))
  }

  /**
    * Creates a function to convert a row to a simple feature. Columns will be mapped to attributes based on
    * matching names.
    *
    * If the row has a `__fid__` column, it will be used for the feature id. Otherwise, it will
    * use a random id prefixed with the current time
    *
    * @param sft simple feature type
    * @param schema dataframe schema
    * @return
    */
  def rowsToFeatures(sft: SimpleFeatureType, schema: StructType): SimpleFeatureRowMapping = {
    val mappings = Seq.tabulate(sft.getAttributeCount)(i => i -> schema.fieldIndex(sft.getDescriptor(i).getLocalName))
    val fid: Row => String = schema.fields.indexWhere(_.name == "__fid__") match {
      case -1 => _ => TimeSortedUuidGenerator.createUuid().toString
      case i  => r => r.getString(i)
    }
    SimpleFeatureRowMapping(sft, mappings, fid)
  }

  /**
    * Creates a function to convert a row to a simple feature, which will be based on the columns in
    * the row schema.
    *
    * If the row has a `__fid__` column, it will be used for the feature id. Otherwise, it will
    * use a random id prefixed with the current time
    *
    * @param name simple feature type name to use
    * @param schema row schema
    * @return
    */
  def rowsToFeatures(name: String, schema: StructType): SimpleFeatureRowMapping =
    rowsToFeatures(createFeatureType(name, schema), schema)

  def sf2row(schema: StructType, sf: SimpleFeature, extractors: Array[SimpleFeature => AnyRef]): Row = {
    val res = Array.ofDim[Any](extractors.length)
    var i = 0
    while(i < extractors.length) {
      res(i) = extractors(i)(sf)
      i += 1
    }
    new GenericRowWithSchema(res, schema)
  }

  def joinedSf2row(schema: StructType, sf1: SimpleFeature, sf2: SimpleFeature, extractors: Array[SimpleFeature => AnyRef]): Row = {
    val leftLength = sf1.getAttributeCount + 1
    val res = Array.ofDim[Any](extractors.length)
    var i = 0
    while(i < leftLength) {
      res(i) = extractors(i)(sf1)
      i += 1
    }
    while(i < extractors.length) {
      res(i) = extractors(i)(sf2)
      i += 1
    }
    new GenericRowWithSchema(res, schema)
  }

  case class SimpleFeatureRowMapping(sft: SimpleFeatureType, mappings: Seq[(Int, Int)], id: Row => String) {
    def apply(row: Row): SimpleFeature = {
      val feature = new ScalaSimpleFeature(sft, id(row))
      mappings.foreach { case (to, from) => feature.setAttributeNoConvert(to, row.getAs[Object](from)) }
      feature
    }
  }
}
