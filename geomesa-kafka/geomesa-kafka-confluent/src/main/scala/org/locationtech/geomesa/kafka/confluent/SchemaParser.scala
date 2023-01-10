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
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
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
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
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
<<<<<<< HEAD
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

<<<<<<< HEAD
import org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.avro.Schema
<<<<<<< HEAD
import org.geotools.api.feature.simple.SimpleFeatureType
=======
=======
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
import org.locationtech.geomesa.utils.geotools.SchemaBuilder
import org.locationtech.geomesa.utils.text.{DateParsing, WKBUtils, WKTUtils}
import org.locationtech.jts.geom._

import java.nio.ByteBuffer
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}
import scala.annotation.tailrec
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

object SchemaParser {

  import scala.collection.JavaConverters._

<<<<<<< HEAD
  private val SkippedPropertyKeys: Set[String] = Set(
    GeoMesaAvroGeomType.KEY,
    GeoMesaAvroGeomFormat.KEY,
    GeoMesaAvroGeomDefault.KEY,
=======
  private val reservedPropertyKeys: Set[String] = Set(
    GeoMesaAvroGeomFormat.KEY,
    GeoMesaAvroGeomType.KEY,
    GeoMesaAvroGeomDefault.KEY,
    GeoMesaAvroDateFormat.KEY,
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
    GeoMesaAvroVisibilityField.KEY,
    GeoMesaAvroExcludeField.KEY
  )

  /**
   * Convert an Avro [[Schema]] into a [[SimpleFeatureType]].
   */
  def schemaToSft(schema: Schema, name: Option[String] = None): SimpleFeatureType = {
    val builder = new SchemaBuilder()

    var defaultGeomField: Option[String] = None
    var visibilityField: Option[String] = None

    schema.getFields.asScala.foreach { field =>
      val metadata = parseMetadata(field)

      metadata.field match {
<<<<<<< HEAD
        case GeometryField(geomType, default) if !metadata.exclude =>
=======
        case GeometryField(_, geomType, default) if !metadata.exclude =>
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
          if (default) {
            defaultGeomField.foreach { name =>
              throw new IllegalArgumentException("There may be only one default geometry field in a schema: " +
                  s"'$name' was already declared as the default")
            }
            defaultGeomField = Some(field.name)
          }

          addGeometryToBuilder(builder, field.name, geomType, default, metadata.extraProps)

        case DateField(_) if !metadata.exclude =>
          builder.addDate(field.name).withOptions(metadata.extraProps.toSeq: _*)

        case VisibilityField =>
          visibilityField.foreach { name =>
            throw new IllegalArgumentException(s"There may be only one visibility field in a schema: " +
                s"'$name' was already declared as the visibility")
          }
          visibilityField = Some(field.name)

          if (!metadata.exclude) {
            addFieldToBuilder(builder, field, userData = metadata.extraProps)
          }
          builder.userData(GeoMesaAvroVisibilityField.KEY, field.name)

        case StandardField if !metadata.exclude =>
          addFieldToBuilder(builder, field, userData = metadata.extraProps)

        case _ =>
      }
    }

    // any extra props on the schema go in the SFT user data
    builder.userData.userData(schema.getObjectProps.asScala.map { case (k, v) => k -> v.toString }.toMap)
    builder.build(name.getOrElse(schema.getName))
  }

  @tailrec
  private def addFieldToBuilder(
      builder: SchemaBuilder,
      field: Schema.Field,
      typeOverride: Option[Schema.Type] = None,
      userData: Map[String, String] = Map.empty): Unit = {
<<<<<<< HEAD
    lazy val logicalType = if (typeOverride.isDefined) { None } else { Option(field.schema().getLogicalType) }
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
    typeOverride.getOrElse(field.schema.getType) match {
      case Schema.Type.STRING  => builder.addString(field.name).withOptions(userData.toSeq: _*)
      case Schema.Type.BOOLEAN => builder.addBoolean(field.name).withOptions(userData.toSeq: _*)
      case Schema.Type.INT     => builder.addInt(field.name).withOptions(userData.toSeq: _*)
      case Schema.Type.DOUBLE  => builder.addDouble(field.name).withOptions(userData.toSeq: _*)
<<<<<<< HEAD
      case Schema.Type.LONG    =>
        logicalType match {
          case Some(_: TimestampMillis) => builder.addDate(field.name).withOptions(userData.toSeq: _*)
          case Some(_: TimestampMicros) => builder.addDate(field.name).withOptions(userData.toSeq: _*)
          case _ => builder.addLong(field.name).withOptions(userData.toSeq: _*)
        }
=======
      case Schema.Type.LONG    => builder.addLong(field.name).withOptions(userData.toSeq: _*)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
      case Schema.Type.FLOAT   => builder.addFloat(field.name).withOptions(userData.toSeq: _*)
      case Schema.Type.BYTES   => builder.addBytes(field.name).withOptions(userData.toSeq: _*)
      case Schema.Type.UNION   =>
        // if a union has more than one non-null type, it is not supported
        val types = field.schema.getTypes.asScala.map(_.getType).filter(_ != Schema.Type.NULL).toSet
        if (types.size != 1) {
          throw UnsupportedAvroTypeException(types.mkString("[", ", ", "]"))
        } else {
          addFieldToBuilder(builder, field, Option(types.head), userData)
        }
      case Schema.Type.ENUM    => builder.addString(field.name).withOptions(userData.toSeq: _*)
      case Schema.Type.MAP     => throw UnsupportedAvroTypeException(Schema.Type.MAP.getName)
      case Schema.Type.RECORD  => throw UnsupportedAvroTypeException(Schema.Type.RECORD.getName)
      case Schema.Type.ARRAY   => throw UnsupportedAvroTypeException(Schema.Type.ARRAY.getName)
      case Schema.Type.FIXED   => throw UnsupportedAvroTypeException(Schema.Type.FIXED.getName)
      case Schema.Type.NULL    => throw UnsupportedAvroTypeException(Schema.Type.NULL.getName)
      case _                   => throw UnsupportedAvroTypeException("unknown")
    }
  }

  private def addGeometryToBuilder(
      builder: SchemaBuilder,
      name: String,
      geometry: Class[_],
      default: Boolean,
      userData: Map[String, String] = Map.empty): Unit = {
    geometry match {
      case t if t == classOf[Point] => builder.addPoint(name, default).withOptions(userData.toSeq: _*)
      case t if t == classOf[LineString] => builder.addLineString(name, default).withOptions(userData.toSeq: _*)
      case t if t == classOf[Polygon] => builder.addPolygon(name, default).withOptions(userData.toSeq: _*)
      case t if t == classOf[MultiPoint] => builder.addMultiPoint(name, default).withOptions(userData.toSeq: _*)
      case t if t == classOf[MultiLineString] => builder.addMultiLineString(name, default).withOptions(userData.toSeq: _*)
      case t if t == classOf[MultiPolygon] => builder.addMultiPolygon(name, default).withOptions(userData.toSeq: _*)
      case t if t == classOf[GeometryCollection] => builder.addGeometryCollection(name, default).withOptions(userData.toSeq: _*)
      case t if t == classOf[Geometry] => builder.addMixedGeometry(name, default).withOptions(userData.toSeq: _*)
      case _ => throw new IllegalArgumentException(s"Unknown geometry type: $geometry")
    }
  }

  private def parseMetadata(field: Schema.Field): GeoMesaAvroMetadata = {
<<<<<<< HEAD
=======
    lazy val geomFormat = GeoMesaAvroGeomFormat.parse(field)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
    lazy val geomType = GeoMesaAvroGeomType.parse(field)
    lazy val geomDefault = GeoMesaAvroGeomDefault.parse(field).getOrElse(false)
    lazy val dateFormat = GeoMesaAvroDateFormat.parse(field)
    lazy val visibility = GeoMesaAvroVisibilityField.parse(field).getOrElse(false)

    val metadata =
<<<<<<< HEAD
      if (geomType.isDefined) {
        GeometryField(geomType.get, geomDefault)
=======
      if (geomFormat.isDefined && geomType.isDefined) {
        GeometryField(geomFormat.get, geomType.get, geomDefault)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
      } else if (dateFormat.isDefined) {
        DateField(dateFormat.get)
      } else if (visibility) {
        VisibilityField
      } else {
        StandardField
      }

    // any field properties that are not one of the defined geomesa avro properties will go in the attribute user data
    val extraProps = field.getObjectProps.asScala.collect {
<<<<<<< HEAD
      case (k, v: String) if !SkippedPropertyKeys.contains(k) => k -> v
=======
      case (k, v: String) if !reservedPropertyKeys.contains(k) => k -> v
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
    }

    val exclude = GeoMesaAvroExcludeField.parse(field).getOrElse(false)

    GeoMesaAvroMetadata(metadata, extraProps.toMap, exclude)
  }

  private case class GeoMesaAvroMetadata(field: GeoMesaAvroField, extraProps: Map[String, String], exclude: Boolean)

  private sealed trait GeoMesaAvroField
<<<<<<< HEAD
  private case class GeometryField(typ: Class[_ <: Geometry], default: Boolean) extends GeoMesaAvroField
=======
  private case class GeometryField(format: String, typ: Class[_ <: Geometry], default: Boolean) extends GeoMesaAvroField
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
  private case class DateField(format: String) extends GeoMesaAvroField
  private case object VisibilityField extends GeoMesaAvroField
  private case object StandardField extends GeoMesaAvroField

  case class UnsupportedAvroTypeException(typeName: String)
    extends IllegalArgumentException(s"Type '$typeName' is not supported for SFT conversion")

  /**
   * A property in an Avro [[Schema.Field]] to provide additional information when generating a [[SimpleFeatureType]].
   *
   * @tparam T the type of the value to be parsed from this property
   */
  trait GeoMesaAvroProperty[T] {
    /**
     * The key in the [[Schema.Field]] for this property.
     */
    val KEY: String

    /**
     * Parse the value from the [[Schema.Field]] properties at this property's `KEY`.
     *
     * @return `None` if the `KEY` does not exist, else the value at the `KEY`
     */
    def parse(field: Schema.Field): Option[T]

    protected final def assertFieldType(field: Schema.Field, typ: Schema.Type): Unit = {
      field.schema.getType match {
        case Schema.Type.UNION =>
          // if a union has more than one non-null type, it should not be converted to an SFT
          val unionTypes = field.schema.getTypes.asScala.map(_.getType).filter(_ != Schema.Type.NULL).toSet
          if (unionTypes.size != 1 || typ != unionTypes.head) {
            throw GeoMesaAvroProperty.InvalidPropertyTypeException(typ.getName, KEY)
          }
        case fieldType: Schema.Type =>
          if (typ != fieldType) {
            throw GeoMesaAvroProperty.InvalidPropertyTypeException(typ.getName, KEY)
          }
      }
    }
  }

  object GeoMesaAvroProperty {
    final case class InvalidPropertyValueException(value: String, key: String)
      extends IllegalArgumentException(s"Unable to parse value '$value' for property '$key'")

    final case class InvalidPropertyTypeException(typeName: String, key: String)
      extends IllegalArgumentException(s"Fields with property '$key' must have type '$typeName'")
  }

  /**
   * A [[GeoMesaAvroProperty]] that has a finite set of possible string values.
   *
   * @tparam T the type of the value to be parsed from this property
   */
  trait GeoMesaAvroEnumProperty[T] extends GeoMesaAvroProperty[T] {
    // case clauses to match the values of the enum and possibly check the field type
    protected val valueMatcher: PartialFunction[(String, Schema.Field), T]

    override final def parse(field: Schema.Field): Option[T] = {
      Option(field.getProp(KEY)).map(_.toLowerCase(Locale.US)).map { value =>
        valueMatcher.lift.apply((value, field)).getOrElse {
          throw GeoMesaAvroProperty.InvalidPropertyValueException(value, KEY)
        }
      }
    }
  }

  /**
   * A [[GeoMesaAvroEnumProperty]] that parses to a boolean value.
   */
  trait GeoMesaAvroBooleanProperty extends GeoMesaAvroEnumProperty[Boolean] {
    final val TRUE: String = "true"
    final val FALSE: String = "false"

    override protected val valueMatcher: PartialFunction[(String, Schema.Field), Boolean] = {
      case (TRUE, _) => true
      case (FALSE, _) => false
    }
  }

  /**
   * A [[GeoMesaAvroEnumProperty]] with a value that can be deserialized from an Avro [[GenericRecord]].
   *
   * @tparam T the type of the value to be parsed from this property
   * @tparam K the type of the value to be deserialized from this property
   */
<<<<<<< HEAD
  @deprecated("deprecated with no replacement")
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
  abstract class GeoMesaAvroDeserializableEnumProperty[T, K: ClassTag] extends GeoMesaAvroEnumProperty[T] {
    // case clauses to match the value of the enum to a function to deserialize the data
    protected val fieldReaderMatcher: PartialFunction[T, AnyRef => K]
    protected val fieldWriterMatcher: PartialFunction[T, K => AnyRef]

<<<<<<< HEAD
    @deprecated("deprecated with no replacement")
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
    final def getFieldReader(schema: Schema, fieldName: String): AnyRef => K = {
      try {
        parse(schema.getField(fieldName)).map { value =>
          if (fieldReaderMatcher.isDefinedAt(value)) {
            fieldReaderMatcher.apply(value)
          } else {
            throw GeoMesaAvroProperty.InvalidPropertyValueException(value.toString, KEY)
          }
        }.getOrElse {
          throw GeoMesaAvroDeserializableEnumProperty.MissingPropertyException(fieldName, KEY)
        }
      } catch {
        case NonFatal(ex) => throw GeoMesaAvroDeserializableEnumProperty.DeserializerException[K](fieldName, ex)
      }
    }

<<<<<<< HEAD
    @deprecated("deprecated with no replacement")
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
    final def getFieldWriter(schema: Schema, fieldName: String): K => AnyRef = {
      try {
        parse(schema.getField(fieldName)).map { value =>
          if (fieldWriterMatcher.isDefinedAt(value)) {
            fieldWriterMatcher.apply(value)
          } else {
            throw GeoMesaAvroProperty.InvalidPropertyValueException(value.toString, KEY)
          }
        }.getOrElse {
          throw GeoMesaAvroDeserializableEnumProperty.MissingPropertyException(fieldName, KEY)
        }
      } catch {
        case NonFatal(ex) => throw GeoMesaAvroDeserializableEnumProperty.DeserializerException[K](fieldName, ex)
      }
    }
  }

<<<<<<< HEAD
  @deprecated("deprecated with no replacement")
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
  object GeoMesaAvroDeserializableEnumProperty {
    final case class MissingPropertyException(fieldName: String, key: String)
      extends RuntimeException(s"Key '$key' is missing from schema for field '$fieldName'")

    final case class DeserializerException[K: ClassTag](fieldName: String, t: Throwable)
      extends RuntimeException(s"Cannot parse deserializer for field '$fieldName' for type " +
        s"'${classTag[K].runtimeClass.getName}': ${t.getMessage}")
  }

  /**
   * Indicates that this field should be interpreted as a [[Geometry]] with the given format. This property must be
   * accompanied by the [[GeoMesaAvroGeomType]] property.
   */
  object GeoMesaAvroGeomFormat extends GeoMesaAvroDeserializableEnumProperty[String, Geometry] {
<<<<<<< HEAD

=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
    override val KEY: String = "geomesa.geom.format"

    /**
     * Well-Known Text representation as a [[String]]
     */
    val WKT: String = "wkt"
    /**
     * Well-Known Bytes representation as an [[Array]] of [[Byte]]s
     */
    val WKB: String = "wkb"

    override protected val valueMatcher: PartialFunction[(String, Schema.Field), String] = {
      case (WKT, field) => assertFieldType(field, Schema.Type.STRING); WKT
      case (WKB, field) => assertFieldType(field, Schema.Type.BYTES); WKB
    }

    override protected val fieldReaderMatcher: PartialFunction[String, AnyRef => Geometry] = {
      case WKT => data => WKTUtils.read(data.toString)
      case WKB => data => WKBUtils.read(unwrap(data.asInstanceOf[ByteBuffer]))
    }

    override protected val fieldWriterMatcher: PartialFunction[String, Geometry => AnyRef] = {
      case WKT => geom => WKTUtils.write(geom)
      case WKB => geom => ByteBuffer.wrap(WKBUtils.write(geom))
    }

    private def unwrap(buf: ByteBuffer): Array[Byte] = {
      if (buf.hasArray && buf.arrayOffset() == 0 && buf.limit() == buf.array().length) {
        buf.array()
      } else {
        val array = Array.ofDim[Byte](buf.limit())
        buf.get(array)
        array
      }
    }
  }

  /**
   * Indicates that this field represents a [[Geometry]] with the given type. This property must be accompanied
   * by the [[GeoMesaAvroGeomFormat]] property.
   */
  object GeoMesaAvroGeomType extends GeoMesaAvroEnumProperty[Class[_ <: Geometry]] {
    override val KEY: String = "geomesa.geom.type"

    /**
     * A [[Geometry]]
     */
    val GEOMETRY: String = "geometry"
    /**
     * A [[Point]]
     */
    val POINT: String = "point"
    /**
     * A [[LineString]]
     */
    val LINESTRING: String = "linestring"
    /**
     * A [[Polygon]]
     */
    val POLYGON: String = "polygon"
    /**
     * A [[MultiPoint]]
     */
    val MULTIPOINT: String = "multipoint"
    /**
     * A [[MultiLineString]]
     */
    val MULTILINESTRING: String = "multilinestring"
    /**
     * A [[MultiPolygon]]
     */
    val MULTIPOLYGON: String = "multipolygon"
    /**
     * A [[GeometryCollection]]
     */
    val GEOMETRYCOLLECTION: String = "geometrycollection"

    override protected val valueMatcher: PartialFunction[(String, Schema.Field), Class[_ <: Geometry]] = {
      case (GEOMETRY, _) => classOf[Geometry]
      case (POINT, _) => classOf[Point]
      case (LINESTRING, _) => classOf[LineString]
      case (POLYGON, _) => classOf[Polygon]
      case (MULTIPOINT, _) => classOf[MultiPoint]
      case (MULTILINESTRING, _) => classOf[MultiLineString]
      case (MULTIPOLYGON, _) => classOf[MultiPolygon]
      case (GEOMETRYCOLLECTION, _) => classOf[GeometryCollection]
    }
  }

  /**
   * Indicates that this field is the default [[Geometry]] for this [[SimpleFeatureType]]. This property must be
   * accompanied by the [[GeoMesaAvroGeomFormat]] and [[GeoMesaAvroGeomType]] properties, and there may only be one
   * of these properties for a given schema.
   */
  object GeoMesaAvroGeomDefault extends GeoMesaAvroBooleanProperty {
    override val KEY: String = "geomesa.geom.default"
  }

  /**
   * Indicates that the field should be interpreted as a [[Date]] in the given format.
   */
  object GeoMesaAvroDateFormat extends GeoMesaAvroDeserializableEnumProperty[String, Date] {
    override val KEY: String = "geomesa.date.format"

    /**
     * Milliseconds since the Unix epoch as a [[Long]]
     */
    val EPOCH_MILLIS: String = "epoch-millis"
    /**
     * A [[String]] with generic ISO date format
     */
    val ISO_DATE: String = "iso-date"
    /**
     * A [[String]] with generic ISO datetime format
     */
    val ISO_DATETIME: String = "iso-datetime"

    override protected val valueMatcher: PartialFunction[(String, Schema.Field), String] = {
      case (EPOCH_MILLIS, field) => assertFieldType(field, Schema.Type.LONG); EPOCH_MILLIS
      case (ISO_DATE, field) => assertFieldType(field, Schema.Type.STRING); ISO_DATE
      case (ISO_DATETIME, field) => assertFieldType(field, Schema.Type.STRING); ISO_DATETIME
    }

    override protected val fieldReaderMatcher: PartialFunction[String, AnyRef => Date] = {
      case EPOCH_MILLIS => data => new Date(data.asInstanceOf[java.lang.Long])
      case ISO_DATE => data => DateParsing.parseDate(data.toString, DateTimeFormatter.ISO_DATE)
      case ISO_DATETIME => data => DateParsing.parseDate(data.toString, DateTimeFormatter.ISO_DATE_TIME)
    }

    override protected val fieldWriterMatcher: PartialFunction[String, Date => AnyRef] = {
      case EPOCH_MILLIS => date => Long.box(date.getTime)
      case ISO_DATE => date => DateParsing.formatDate(date, DateTimeFormatter.ISO_DATE)
      case ISO_DATETIME => date => DateParsing.formatDate(date, DateTimeFormatter.ISO_DATE_TIME)
    }
  }

  /**
   * Specifies that the value of this field should be used as the visibility for features of this [[SimpleFeatureType]].
   * There may only be one of these properties for a given schema.
   */
  object GeoMesaAvroVisibilityField extends GeoMesaAvroBooleanProperty {
    override val KEY: String = "geomesa.visibility.field"

    override protected val valueMatcher: PartialFunction[(String, Schema.Field), Boolean] = {
      case (TRUE, field) => assertFieldType(field, Schema.Type.STRING); true
      case (FALSE, field) => assertFieldType(field, Schema.Type.STRING); false
    }
  }

  /**
   * Specifies whether this field should be excluded from the [[SimpleFeatureType]].
   */
  object GeoMesaAvroExcludeField extends GeoMesaAvroBooleanProperty {
    override val KEY: String = "geomesa.exclude.field"
  }
}
