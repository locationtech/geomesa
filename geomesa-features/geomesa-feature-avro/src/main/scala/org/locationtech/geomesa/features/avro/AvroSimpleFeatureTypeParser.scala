/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.locationtech.jts.geom.{Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon}
import org.opengis.feature.simple.SimpleFeatureType

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}
import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions._
import scala.reflect.{ClassTag, classTag}
import scala.util.Try

object AvroSimpleFeatureTypeParser {
  /**
   * Convert an Avro [[Schema]] into a [[SimpleFeatureType]].
   */
  def schemaToSft(schema: Schema): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder
    builder.setName(schema.getName)
    schema.getFields.foreach { field =>
      val fieldName = field.name()
      val metadata = parseMetadata(field)
      metadata match {
        case GeomMetadata(format, geomType, default) =>
          builder.add(fieldName, geomType)
          builder.get(fieldName).getUserData.put(GeomesaAvroGeomFormat.KEY, format)
          // if more than one field is set as the default geometry, the last one becomes the default
          if (default) {
            builder.setDefaultGeometry(fieldName)
          }
        case DateMetadata(format) =>
          builder.add(fieldName, classOf[Date])
          builder.get(fieldName).getUserData.put(GeomesaAvroDateFormat.KEY, format)
        case NoMetadata =>
          addFieldToBuilder(builder, field)
      }
    }
    builder.buildFeatureType()
  }

  @tailrec
  private def addFieldToBuilder(builder: SimpleFeatureTypeBuilder,
                                field: Schema.Field,
                                typeOverride: Option[Schema.Type] = None): Unit = {
    typeOverride.getOrElse(field.schema().getType) match {
      case Schema.Type.STRING  => builder.add(field.name(), classOf[java.lang.String])
      case Schema.Type.BOOLEAN => builder.add(field.name(), classOf[java.lang.Boolean])
      case Schema.Type.INT     => builder.add(field.name(), classOf[java.lang.Integer])
      case Schema.Type.DOUBLE  => builder.add(field.name(), classOf[java.lang.Double])
      case Schema.Type.LONG    => builder.add(field.name(), classOf[java.lang.Long])
      case Schema.Type.FLOAT   => builder.add(field.name(), classOf[java.lang.Float])
      case Schema.Type.BYTES   => builder.add(field.name(), classOf[Array[Byte]])
      case Schema.Type.UNION   =>
        // if a union has more than one non-null type, it is not supported
        val types = field.schema().getTypes.map(_.getType).filter(_ != Schema.Type.NULL).toSet
        if (types.size != 1) {
          throw UnsupportedAvroTypeException(types.mkString("[", ", ", "]"))
        } else {
          addFieldToBuilder(builder, field, Option(types.head))
        }
      case Schema.Type.MAP     => throw UnsupportedAvroTypeException(Schema.Type.MAP.getName)
      case Schema.Type.RECORD  => throw UnsupportedAvroTypeException(Schema.Type.RECORD.getName)
      case Schema.Type.ENUM    => builder.add(field.name(), classOf[java.lang.String])
      case Schema.Type.ARRAY   => throw UnsupportedAvroTypeException(Schema.Type.ARRAY.getName)
      case Schema.Type.FIXED   => throw UnsupportedAvroTypeException(Schema.Type.FIXED.getName)
      case Schema.Type.NULL    => throw UnsupportedAvroTypeException(Schema.Type.NULL.getName)
      case _                   => throw UnsupportedAvroTypeException("unknown")
    }
  }

  private def parseMetadata(field: Schema.Field): GeomesaAvroFieldMetadata = {
    val geomFormat = GeomesaAvroGeomFormat.parse(field)
    val geomType = GeomesaAvroGeomType.parse(field)
    val geomDefault = GeomesaAvroGeomDefault.parse(field)
    val dateFormat = GeomesaAvroDateFormat.parse(field)

    if (geomFormat.isDefined && geomType.isDefined) {
      GeomMetadata(geomFormat.get, geomType.get, geomDefault.getOrElse(false))
    } else if (dateFormat.isDefined) {
      DateMetadata(dateFormat.get)
    } else {
      NoMetadata
    }
  }

  sealed trait GeomesaAvroFieldMetadata
  case class GeomMetadata(format: String, typ: Class[_ <: Geometry], default: Boolean) extends GeomesaAvroFieldMetadata
  case class DateMetadata(format: String) extends GeomesaAvroFieldMetadata
  case object NoMetadata extends GeomesaAvroFieldMetadata

  case class UnsupportedAvroTypeException(typeName: String)
    extends IllegalArgumentException(s"Type '$typeName' is not supported for SFT conversion")

  // an attribute in an avro schema to provide additional information when creating an SFT
  trait GeomesaAvroProperty[T] {
    val KEY: String

    // parse the value from the schema field at this property's key, returning none if the key does not exist,
    // and throwing an exception if the value cannot be parsed
    def parse(field: Schema.Field): Option[T]

    protected final def assertFieldType(field: Schema.Field, typ: Schema.Type): Unit = {
      field.schema.getType match {
        case Schema.Type.UNION =>
          // if a union has more than one non-null type, it should not be converted to an SFT
          val unionTypes = field.schema.getTypes.map(_.getType).filter(_ != Schema.Type.NULL).toSet
          if (unionTypes.size != 1 || typ != unionTypes.head) {
            throw GeomesaAvroProperty.InvalidPropertyTypeException(typ.getName, KEY)
          }
        case fieldType: Schema.Type =>
          if (typ != fieldType) {
            throw GeomesaAvroProperty.InvalidPropertyTypeException(typ.getName, KEY)
          }
      }
    }
  }

  object GeomesaAvroProperty {
    final case class InvalidPropertyValueException(value: String, key: String)
      extends IllegalArgumentException(s"Unable to parse value '$value' for property '$key'")

    final case class InvalidPropertyTypeException(typeName: String, key: String)
      extends IllegalArgumentException(s"Fields with property '$key' must have type '$typeName'")
  }

  trait GeomesaAvroEnumProperty[T] extends GeomesaAvroProperty[T] {
    // the values that can be parsed for this property
    protected val SUPPORTED: Set[String]

    final def supported(value: String): Option[String] =
      Option(value.toUpperCase(Locale.ENGLISH)).filter(SUPPORTED.contains)
    final def supportedOrThrow(value: String): String = {
      supported(value).getOrElse {
        throw GeomesaAvroProperty.InvalidPropertyValueException(value, KEY)
      }
    }
  }

  trait GeomesaAvroEnumPropertyWithDeserialization[K, T] extends GeomesaAvroEnumProperty[K] {
    protected implicit val ctg: ClassTag[T] = classTag[T]

    // deserialize the value from an avro record field based on a geomesa avro enum property value
    def deserialize(record: GenericRecord, fieldName: String, enumValue: String): T

    final case class DeserializationException(fieldName: String, t: Throwable)
      extends RuntimeException(s"Could not deserialize field '$fieldName' into a ${ctg.runtimeClass.getName}: ", t)
  }

  object GeomesaAvroGeomDefault extends GeomesaAvroProperty[Boolean] {
    override val KEY: String = "geomesa.geom.default"

    override def parse(field: Schema.Field): Option[Boolean] = {
      Option(field.getProp(KEY)).map { geomDefault =>
        Try(geomDefault.toBoolean).getOrElse {
          throw GeomesaAvroProperty.InvalidPropertyValueException(geomDefault, KEY)
        }
      }
    }
  }

  object GeomesaAvroGeomFormat extends GeomesaAvroEnumPropertyWithDeserialization[String, Geometry] {
    override val KEY: String = "geomesa.geom.format"

    val WKT: String = "WKT"
    val WKB: String = "WKB"

    override protected val SUPPORTED: Set[String] = Set(WKT, WKB)

    override def parse(field: Schema.Field): Option[String] = {
      Option(field.getProp(KEY)).map(supportedOrThrow(_) match {
        case WKT => assertFieldType(field, Schema.Type.STRING); WKT
        case WKB => assertFieldType(field, Schema.Type.BYTES); WKB
      })
    }

    override def deserialize(record: GenericRecord, fieldName: String, enumValue: String): Geometry = {
      try {
        val data = record.get(fieldName)
        supportedOrThrow(enumValue) match {
          case WKT => WKTUtils.read(data.asInstanceOf[String])
          case WKB => WKBUtils.read(data.asInstanceOf[Array[Byte]])
        }
      } catch {
        case ex: Exception => throw DeserializationException(fieldName, ex)
      }
    }
  }

  object GeomesaAvroGeomType extends GeomesaAvroEnumProperty[Class[_ <: Geometry]] {
    override val KEY: String = "geomesa.geom.type"

    val GEOMETRY: String = "GEOMETRY"
    val POINT: String = "POINT"
    val LINESTRING: String = "LINESTRING"
    val POLYGON: String = "POLYGON"
    val MULTIPOINT: String = "MULTIPOINT"
    val MULTILINESTRING: String = "MULTILINESTRING"
    val MULTIPOLYGON: String = "MULTIPOLYGON"
    val GEOMETRYCOLLECTION: String = "GEOMETRYCOLLECTION"

    override protected val SUPPORTED: Set[String] =
      Set(GEOMETRY, POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION)

    override def parse(field: Schema.Field): Option[Class[_ <: Geometry]] = {
      Option(field.getProp(KEY)).map(supportedOrThrow(_) match {
        case GEOMETRY => classOf[Geometry]
        case POINT => classOf[Point]
        case LINESTRING => classOf[LineString]
        case POLYGON => classOf[Polygon]
        case MULTIPOINT => classOf[MultiPoint]
        case MULTILINESTRING => classOf[MultiLineString]
        case MULTIPOLYGON => classOf[MultiPolygon]
        case GEOMETRYCOLLECTION => classOf[GeometryCollection]
      })
    }
  }

  object GeomesaAvroDateFormat extends GeomesaAvroEnumPropertyWithDeserialization[String, Date] {
    override val KEY: String = "geomesa.date.format"

    val EPOCH_MILLIS: String = "EPOCH_MILLIS"
    val ISO_DATE: String = "ISO_DATE"
    val ISO_DATETIME_OFFSET: String = "ISO_DATETIME_OFFSET"
    val ISO_INSTANT: String = "ISO_INSTANT"

    override protected val SUPPORTED: Set[String] = Set(EPOCH_MILLIS, ISO_DATE, ISO_DATETIME_OFFSET, ISO_INSTANT)

    override def parse(field: Schema.Field): Option[String] = {
      Option(field.getProp(KEY)).map(supportedOrThrow(_) match {
        case EPOCH_MILLIS => assertFieldType(field, Schema.Type.LONG); EPOCH_MILLIS
        case ISO_DATE => assertFieldType(field, Schema.Type.STRING); ISO_DATE
        case ISO_DATETIME_OFFSET => assertFieldType(field, Schema.Type.STRING); ISO_DATETIME_OFFSET
        case ISO_INSTANT => assertFieldType(field, Schema.Type.STRING); ISO_INSTANT
      })
    }

    override def deserialize(record: GenericRecord, fieldName: String, enumValue: String): Date = {
      try {
        val data = record.get(fieldName)
        supportedOrThrow(enumValue) match {
          case EPOCH_MILLIS =>
            new Date(data.asInstanceOf[java.lang.Long])
          case ISO_DATE =>
            Date.from(Instant.from(DateTimeFormatter.ISO_DATE.parse(data.asInstanceOf[String])))
          case ISO_DATETIME_OFFSET =>
            Date.from(Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(data.asInstanceOf[String])))
          case ISO_INSTANT =>
            Date.from(Instant.from(DateTimeFormatter.ISO_INSTANT.parse(data.asInstanceOf[String])))
        }
      } catch {
        case ex: Exception => throw DeserializationException(fieldName, ex)
      }
    }
  }
}
