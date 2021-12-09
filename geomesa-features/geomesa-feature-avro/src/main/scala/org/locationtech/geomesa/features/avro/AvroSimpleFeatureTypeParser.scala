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
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeParser.GeomesaAvroProperty.DeserializationException
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.locationtech.jts.geom.{Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon}
import org.opengis.feature.simple.SimpleFeatureType

import java.util.{Date, Locale}
import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions._
import scala.reflect.ClassTag
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
          if (default) { builder.setDefaultGeometry(fieldName) }
        case DateMetadata(format) =>
          builder.add(fieldName, classOf[Date])
          builder.get(fieldName).getUserData.put(GeomesaAvroDateFormat.KEY, format)
        case FeatureVisibilityMetadata =>
          addFieldToBuilder(builder, field)
          builder.get(fieldName).getUserData.put(GeomesaAvroFeatureVisibility.KEY, "")
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
    // all fields metadata fields are parsed before their value is examined, e.g. a geom field cannot have an
    // invalid date format, and, if it has valid date format metadata, it will be ignored
    val geomFormat = GeomesaAvroGeomFormat.parse(field)
    val geomType = GeomesaAvroGeomType.parse(field)
    val geomDefault = GeomesaAvroGeomDefault.parse(field)
    val dateFormat = GeomesaAvroDateFormat.parse(field)
    val featureVisibility = GeomesaAvroFeatureVisibility.parse(field)

    if (geomFormat.isDefined && geomType.isDefined) {
      GeomMetadata(geomFormat.get, geomType.get, geomDefault.getOrElse(false))
    } else if (dateFormat.isDefined) {
      DateMetadata(dateFormat.get)
    } else if (featureVisibility.isDefined) {
      FeatureVisibilityMetadata
    } else {
      NoMetadata
    }
  }

  private sealed trait GeomesaAvroFieldMetadata
  private case class GeomMetadata(format: String, typ: Class[_ <: Geometry], default: Boolean)
    extends GeomesaAvroFieldMetadata
  private case class DateMetadata(format: String) extends GeomesaAvroFieldMetadata
  private case object FeatureVisibilityMetadata extends GeomesaAvroFieldMetadata
  private case object NoMetadata extends GeomesaAvroFieldMetadata

  case class UnsupportedAvroTypeException(typeName: String)
    extends IllegalArgumentException(s"Type '$typeName' is not supported for SFT conversion")

  /**
   * An attribute in an Avro [[Schema.Field]] to provide additional information when creating an SFT.
   */
  trait GeomesaAvroProperty[T] {
    /**
     * The key in the [[Schema.Field]] for this attribute.
     */
    val KEY: String

    /**
     * Parse the value from the [[Schema.Field]] at this property's `key`.
     *
     * @return `None` if the `key` does not exist, else the value at the `key`
     */
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

    final case class MissingPropertyValueException[T](fieldName: String, key: String)(implicit ctg: ClassTag[T])
      extends IllegalArgumentException(s"Cannot process field '$fieldName' for type '${ctg.runtimeClass.getName} " +
        s"because key '$key' is missing")

    final case class DeserializationException[T](fieldName: String, t: Throwable)(implicit ctg: ClassTag[T])
      extends RuntimeException(s"Cannot deserialize field '$fieldName' into a ${ctg.runtimeClass.getName}: " +
        s"${t.getMessage}")
  }

  /**
   * Indicates that this avro field should be interpreted as the default [[Geometry]] for this [[SimpleFeatureType]].
   */
  object GeomesaAvroGeomDefault extends GeomesaAvroProperty[Boolean] {
    override val KEY: String = "geomesa.geom.default"

    val TRUE: String = "TRUE"
    val FALSE: String = "FALSE"

    override def parse(field: Schema.Field): Option[Boolean] = {
      Option(field.getProp(KEY)).map(_.toUpperCase(Locale.ENGLISH)).map {
        case TRUE => true
        case FALSE => false
        case value: String => throw GeomesaAvroProperty.InvalidPropertyValueException(value, KEY)
      }
    }
  }

  /**
   * Indicates that the avro field should be interpreted as a [[Geometry]], with one of the formats specified below.
   */
  object GeomesaAvroGeomFormat extends GeomesaAvroProperty[String] {
    override val KEY: String = "geomesa.geom.format"

    /**
     * Well-Known Text representation as a [[String]]
     */
    val WKT: String = "WKT"
    /**
     * Well-Known Bytes representation as an [[Array]] of [[Byte]]s
     */
    val WKB: String = "WKB"

    override def parse(field: Schema.Field): Option[String] = {
      Option(field.getProp(KEY)).map(_.toUpperCase(Locale.ENGLISH)).map {
        case WKT => assertFieldType(field, Schema.Type.STRING); WKT
        case WKB => assertFieldType(field, Schema.Type.BYTES); WKB
        case value: String => throw GeomesaAvroProperty.InvalidPropertyValueException(value, KEY)
      }
    }

    /**
     * Convert a field in a [[GenericRecord]] to a [[Geometry]] based on the [[GeomesaAvroGeomFormat]] type.
     */
    def deserialize(record: GenericRecord, fieldName: String, format: String): Geometry = {
      val data = record.get(fieldName)
      if (data == null) { return null }
      try {
        format.toUpperCase(Locale.ENGLISH) match {
          case WKT => WKTUtils.read(data.toString)
          case WKB => WKBUtils.read(data.asInstanceOf[Array[Byte]])
          case value: String => throw GeomesaAvroProperty.InvalidPropertyValueException(value, KEY)
        }
      } catch {
        case ex: Exception => throw DeserializationException[Geometry](fieldName, ex)
      }
    }
  }

  /**
   * Indicates that the avro field represents a [[Geometry]], with one of the types specified below.
   */
  object GeomesaAvroGeomType extends GeomesaAvroProperty[Class[_ <: Geometry]] {
    override val KEY: String = "geomesa.geom.type"

    /**
     * A [[Geometry]]
     */
    val GEOMETRY: String = "GEOMETRY"
    /**
     * A [[Point]]
     */
    val POINT: String = "POINT"
    /**
     * A [[LineString]]
     */
    val LINESTRING: String = "LINESTRING"
    /**
     * A [[Polygon]]
     */
    val POLYGON: String = "POLYGON"
    /**
     * A [[MultiPoint]]
     */
    val MULTIPOINT: String = "MULTIPOINT"
    /**
     * A [[MultiLineString]]
     */
    val MULTILINESTRING: String = "MULTILINESTRING"
    /**
     * A [[MultiPolygon]]
     */
    val MULTIPOLYGON: String = "MULTIPOLYGON"
    /**
     * A [[GeometryCollection]]
     */
    val GEOMETRYCOLLECTION: String = "GEOMETRYCOLLECTION"

    override def parse(field: Schema.Field): Option[Class[_ <: Geometry]] = {
      Option(field.getProp(KEY)).map(_.toUpperCase(Locale.ENGLISH)).map {
        case GEOMETRY => classOf[Geometry]
        case POINT => classOf[Point]
        case LINESTRING => classOf[LineString]
        case POLYGON => classOf[Polygon]
        case MULTIPOINT => classOf[MultiPoint]
        case MULTILINESTRING => classOf[MultiLineString]
        case MULTIPOLYGON => classOf[MultiPolygon]
        case GEOMETRYCOLLECTION => classOf[GeometryCollection]
        case value: String => throw GeomesaAvroProperty.InvalidPropertyValueException(value, KEY)
      }
    }
  }

  /**
   * Indicates that the avro field should be interpreted as a [[Date]], with one of the formats specified below.
   */
  object GeomesaAvroDateFormat extends GeomesaAvroProperty[String] {
    override val KEY: String = "geomesa.date.format"

    /**
     * Milliseconds since the Unix epoch as a [[Long]]
     */
    val EPOCH_MILLIS: String = "EPOCH_MILLIS"
    /**
     * A [[String]] with date format "yyyy-MM-dd"
     */
    val ISO_DATE: String = "ISO_DATE"
    /**
     * A [[String]] with date format "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"
     */
    val ISO_INSTANT: String = "ISO_INSTANT"

    override def parse(field: Schema.Field): Option[String] = {
      Option(field.getProp(KEY)).map(_.toUpperCase(Locale.ENGLISH)).map {
        case EPOCH_MILLIS => assertFieldType(field, Schema.Type.LONG); EPOCH_MILLIS
        case ISO_DATE => assertFieldType(field, Schema.Type.STRING); ISO_DATE
        case ISO_INSTANT => assertFieldType(field, Schema.Type.STRING); ISO_INSTANT
        case value: String => throw GeomesaAvroProperty.InvalidPropertyValueException(value, KEY)
      }
    }

    /**
     * Convert a field in a [[GenericRecord]] to a [[Date]] based on the [[GeomesaAvroDateFormat]] type.
     */
    def deserialize(record: GenericRecord, fieldName: String, format: String): Date = {
      try {
        val data = record.get(fieldName)
        if (data == null) { return null }
        format.toUpperCase(Locale.ENGLISH) match {
          case EPOCH_MILLIS => new Date(data.asInstanceOf[java.lang.Long])
          case ISO_DATE => ISODateTimeFormat.date().parseDateTime(data.toString).toDate
          case ISO_INSTANT => ISODateTimeFormat.dateTime().parseDateTime(data.toString).toDate
          case value: String => throw GeomesaAvroProperty.InvalidPropertyValueException(value, KEY)
        }
      } catch {
        case ex: Exception => throw DeserializationException[Date](fieldName, ex)
      }
    }
  }

  /**
   * Indicates that this avro field should be interpreted as the visibility attribute for this [[SimpleFeatureType]].
   */
  object GeomesaAvroFeatureVisibility extends GeomesaAvroProperty[Unit] {
    override val KEY: String = "geomesa.feature.visibility"

    override def parse(field: Schema.Field): Option[Unit] = {
      Option(field.getProp(KEY)).map(_ => assertFieldType(field, Schema.Type.STRING))
    }
  }
}
