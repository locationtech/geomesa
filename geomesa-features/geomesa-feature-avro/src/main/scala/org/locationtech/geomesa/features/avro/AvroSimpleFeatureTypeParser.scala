/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import org.apache.avro.Schema
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.jts.geom.{Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon}
import org.opengis.feature.simple.SimpleFeatureType

import java.util.{Date, Locale}
import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions._
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

  trait GeomesaAvroPropertyEnum[T] extends GeomesaAvroProperty[T] {
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

  object GeomesaAvroGeomFormat extends GeomesaAvroPropertyEnum[String] {
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
  }

  object GeomesaAvroGeomType extends GeomesaAvroPropertyEnum[Class[_ <: Geometry]] {
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

  object GeomesaAvroDateFormat extends GeomesaAvroPropertyEnum[String] {
    override val KEY: String = "geomesa.date.format"

    val ISO8601: String = "ISO8601"

    override protected val SUPPORTED: Set[String] = Set(ISO8601)

    override def parse(field: Schema.Field): Option[String] = {
      Option(field.getProp(KEY)).map(supportedOrThrow(_) match {
        case ISO8601 => assertFieldType(field, Schema.Type.STRING); ISO8601
      })
    }
  }
}
