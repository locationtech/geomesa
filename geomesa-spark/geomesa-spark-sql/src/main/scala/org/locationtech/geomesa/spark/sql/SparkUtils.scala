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
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.FilterFactory
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpec.{ListAttributeSpec, MapAttributeSpec}
import org.locationtech.geomesa.utils.uuid.TimeSortedUuidGenerator

import java.sql.Timestamp
import java.util.Date

object SparkUtils extends LazyLogging {

  import scala.collection.JavaConverters._

  @transient
  val ff: FilterFactory = CommonFactoryFinder.getFilterFactory

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
        fieldType match {
          case _: TimestampType =>
            sf: SimpleFeature => {
              val attr = sf.getAttribute(index)
              if (attr == null) { null } else {
                new Timestamp(attr.asInstanceOf[Date].getTime)
              }
            }

          case arrayType: ArrayType =>
            val elementType = arrayType.elementType
            sf: SimpleFeature => {
              val attr = sf.getAttribute(index)
              if (attr == null) { null } else {
                val array = attr.asInstanceOf[java.util.List[_]].asScala.toList
                if (elementType != TimestampType) { array } else {
                  array.map(d => new Timestamp(d.asInstanceOf[Date].getTime))
                }
              }
            }

          case mapType: MapType =>
            val keyType = mapType.keyType
            val valueType = mapType.valueType
            sf: SimpleFeature => {
              val attr = sf.getAttribute(index)
              if (attr == null) { null } else {
                val map = attr.asInstanceOf[java.util.Map[Any, Any]].asScala.toMap
                if (keyType != TimestampType && valueType != TimestampType) { map } else {
                  map.map {
                    case (key, value) =>
                      val newKey = if (keyType == TimestampType) {
                        new Timestamp(key.asInstanceOf[Date].getTime)
                      } else key
                      val newValue = if (valueType == TimestampType) {
                        new Timestamp(value.asInstanceOf[Date].getTime)
                      } else value
                      (newKey, newValue)
                  }
                }
              }
            }

          case _ => sf: SimpleFeature => sf.getAttribute(index)
        }
    }
  }

  def sparkFilterToCQLFilter(filt: org.apache.spark.sql.sources.Filter): Option[org.geotools.api.filter.Filter] = filt match {
    case GreaterThanOrEqual(attribute, v) => Some(ff.greaterOrEqual(ff.property(attribute), ff.literal(v)))
    case GreaterThan(attr, v)             => Some(ff.greater(ff.property(attr), ff.literal(v)))
    case LessThanOrEqual(attr, v)         => Some(ff.lessOrEqual(ff.property(attr), ff.literal(v)))
    case LessThan(attr, v)                => Some(ff.less(ff.property(attr), ff.literal(v)))
    case EqualTo(attr, v) if attr == "__fid__" => Some(ff.id(ff.featureId(v.toString)))
    case EqualTo(attr, v)                      => Some(ff.equals(ff.property(attr), ff.literal(v)))
    case In(attr, values) if attr == "__fid__" => Some(ff.id(values.map(v => ff.featureId(v.toString)).toSet.asJava))
    case In(attr, values)                      =>
      Some(values.map(v => ff.equals(ff.property(attr), ff.literal(v))).reduce[org.geotools.api.filter.Filter]( (l,r) => ff.or(l,r)))
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

    def basicTypeBinding(dataType: DataType): Class[_] = dataType match {
      case DataTypes.StringType              => classOf[java.lang.String]
      case DataTypes.DateType                => classOf[java.util.Date]
      case DataTypes.TimestampType           => classOf[java.util.Date]
      case DataTypes.IntegerType             => classOf[java.lang.Integer]
      case DataTypes.LongType                => classOf[java.lang.Long]
      case DataTypes.FloatType               => classOf[java.lang.Float]
      case DataTypes.DoubleType              => classOf[java.lang.Double]
      case DataTypes.BooleanType             => classOf[java.lang.Boolean]
      case DataTypes.BinaryType              => classOf[Array[Byte]]
      case _                                 => null
    }

    def geomTypeBinding(dataType: DataType): Class[_] = dataType match {
      case JTSTypes.PointTypeInstance        => classOf[org.locationtech.jts.geom.Point]
      case JTSTypes.MultiPointTypeInstance   => classOf[org.locationtech.jts.geom.MultiPoint]
      case JTSTypes.LineStringTypeInstance   => classOf[org.locationtech.jts.geom.LineString]
      case JTSTypes.MultiLineStringTypeInstance => classOf[org.locationtech.jts.geom.MultiLineString]
      case JTSTypes.PolygonTypeInstance      => classOf[org.locationtech.jts.geom.Polygon]
      case JTSTypes.MultipolygonTypeInstance => classOf[org.locationtech.jts.geom.MultiPolygon]
      case JTSTypes.GeometryCollectionTypeInstance => classOf[org.locationtech.jts.geom.GeometryCollection]
      case JTSTypes.GeometryTypeInstance     => classOf[org.locationtech.jts.geom.Geometry]
      case _                                 => null
    }

    struct.fields.filter(_.name != "__fid__").foreach { field =>
      field.dataType match {
        case ArrayType(elementType, _) =>
          val elementBinding = basicTypeBinding(elementType)
          if (elementBinding == null) {
            throw new IllegalArgumentException(
              s"list element in field ${field.name} is not basic type: ${elementType.typeName}")
          }
          val attributeSpec = ListAttributeSpec(field.name, elementBinding, Map.empty)
          builder.add(attributeSpec.toDescriptor)

        case MapType(keyType, valueType, _) =>
          val keyBinding = basicTypeBinding(keyType)
          if (keyBinding == null) {
            throw new IllegalArgumentException(
              s"map key in field ${field.name} is not basic type: ${keyType.typeName}")
          }
          val valueBinding = basicTypeBinding(valueType)
          if (valueBinding == null) {
            throw new IllegalArgumentException(
              s"map value in field ${field.name} is not basic type: ${valueType.typeName}")
          }
          val attributeSpec = MapAttributeSpec(field.name, keyBinding, valueBinding, Map.empty)
          builder.add(attributeSpec.toDescriptor)

        case _ =>
          var binding = basicTypeBinding(field.dataType)
          if (binding == null) binding = geomTypeBinding(field.dataType)
          if (binding == null) {
            throw new IllegalArgumentException(
              s"Unexpected data type for field ${field.name}: ${field.dataType.typeName}")
          }
          builder.add(field.name, binding)
      }
    }

    builder.buildFeatureType()
  }

  def createStructType(sft: SimpleFeatureType): StructType = {
    val fields = sft.getAttributeDescriptors.asScala.flatMap(createStructField).toList
    StructType(StructField("__fid__", DataTypes.StringType, nullable = false) :: fields)
  }

  private def createStructField(ad: AttributeDescriptor): Option[StructField] = {
    def basicTypeToSQLType(bindings: Seq[ObjectType.ObjectType]): Option[DataType] = {
      bindings.head match {
        case ObjectType.STRING   => Some(DataTypes.StringType)
        case ObjectType.INT      => Some(DataTypes.IntegerType)
        case ObjectType.LONG     => Some(DataTypes.LongType)
        case ObjectType.FLOAT    => Some(DataTypes.FloatType)
        case ObjectType.DOUBLE   => Some(DataTypes.DoubleType)
        case ObjectType.BOOLEAN  => Some(DataTypes.BooleanType)
        case ObjectType.DATE     => Some(DataTypes.TimestampType)
        case ObjectType.BYTES    => Some(DataTypes.BinaryType)
        case ObjectType.UUID     => None // not supported
        case _                   => None // not basic type
      }
    }

    def geomTypeToSQLType(bindings: Seq[ObjectType.ObjectType]): DataType = {
      bindings.last match {
        case ObjectType.POINT               => JTSTypes.PointTypeInstance
        case ObjectType.LINESTRING          => JTSTypes.LineStringTypeInstance
        case ObjectType.POLYGON             => JTSTypes.PolygonTypeInstance
        case ObjectType.MULTIPOINT          => JTSTypes.MultiPointTypeInstance
        case ObjectType.MULTILINESTRING     => JTSTypes.MultiLineStringTypeInstance
        case ObjectType.MULTIPOLYGON        => JTSTypes.MultipolygonTypeInstance
        case ObjectType.GEOMETRY_COLLECTION => JTSTypes.GeometryCollectionTypeInstance
        case _                              => JTSTypes.GeometryTypeInstance
      }
    }

    def listTypeToSQLType(bindings: Seq[ObjectType.ObjectType]): Option[DataType] =
      basicTypeToSQLType(bindings.tail).map(ArrayType(_))

    def mapTypeToSQLType(bindings: Seq[ObjectType.ObjectType]): Option[DataType] = {
      (basicTypeToSQLType(bindings.tail), basicTypeToSQLType(bindings.tail.tail)) match {
        case (Some(keyType), Some(valueType)) => Some(MapType(keyType, valueType))
        case _ => None
      }
    }

    val bindings = ObjectType.selectType(ad)
    val dt = bindings.head match {
      case ObjectType.STRING   |
           ObjectType.INT      |
           ObjectType.LONG     |
           ObjectType.FLOAT    |
           ObjectType.DOUBLE   |
           ObjectType.BOOLEAN  |
           ObjectType.DATE     |
           ObjectType.UUID     |
           ObjectType.BYTES    => basicTypeToSQLType(bindings)
      case ObjectType.LIST     => listTypeToSQLType(bindings)
      case ObjectType.MAP      => mapTypeToSQLType(bindings)
      case ObjectType.GEOMETRY => Some(geomTypeToSQLType(bindings))
      case _ => logger.warn(s"Unexpected bindings for descriptor $ad: ${bindings.mkString(", ")}"); None
    }
    dt.map(StructField(ad.getLocalName, _))
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
    val mappings = Seq.tabulate(sft.getAttributeCount) { i =>
      val descriptor = sft.getDescriptor(i)
      val binding = descriptor.getType.getBinding
      val needConversion = (classOf[java.util.List[_]].isAssignableFrom(binding) ||
        classOf[java.util.Map[_, _]].isAssignableFrom(binding))
      (i, schema.fieldIndex(descriptor.getLocalName), needConversion)
    }
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

  case class SimpleFeatureRowMapping(sft: SimpleFeatureType, mappings: Seq[(Int, Int, Boolean)], id: Row => String) {
    def apply(row: Row): SimpleFeature = {
      val feature = new ScalaSimpleFeature(sft, id(row))
      mappings.foreach { case (to, from, needConversion) =>
        if (needConversion) feature.setAttribute(to, row.getAs[Object](from))
        else feature.setAttributeNoConvert(to, row.getAs[Object](from))
      }
      feature
    }
  }
}
