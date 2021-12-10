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
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.locationtech.jts.geom.{Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon}
import org.opengis.feature.simple.SimpleFeatureType

import java.nio.ByteBuffer
import java.util.{Date, Locale}
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.reflect.{ClassTag, classTag}

object AvroSimpleFeatureTypeParser {
  /**
   * Convert an Avro [[Schema]] into a [[SimpleFeatureType]].
   */
  def schemaToSft(schema: Schema): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder
    builder.setName(schema.getName)

    // any additional top-level props on the schema go in the sft user data
    val userData = schema.getProps

    schema.getFields.foreach { field =>
      val fieldName = field.name()
      val metadata = parseMetadata(field)

      metadata.properties match {
        case GeomProperties(_, geomType, default, srid) =>
          builder.add(fieldName, geomType)
          // if more than one field is set as the default geometry, the last one becomes the default
          if (default) { builder.setDefaultGeometry(fieldName) }
          srid.map(builder.get(fieldName).getUserData.put(SimpleFeatureTypes.AttributeOptions.OptSrid, _))
        case DateProperties(_, default) =>
          builder.add(fieldName, classOf[Date])
          // if more than one field is set as the default date, the last one becomes the default
          if (default) { userData.put(SimpleFeatureTypes.Configs.DefaultDtgField, fieldName) }
        case VisibilityProperties =>
          // if there is more than one visibilities field, the last one is used
          userData.put(GeomesaAvroFeatureVisibility.KEY, fieldName)
          addFieldToBuilder(builder, field)
        case NoProperties =>
          addFieldToBuilder(builder, field)
      }

      // any extra props on the field go in the attribute user data
      builder.get(fieldName).getUserData.putAll(metadata.extra)
    }

    val sft = builder.buildFeatureType()
    sft.getUserData.putAll(userData)
    sft
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
    // all fields metadata fields are parsed before their value is examined and the metadata values are returned
    // with the following precedence: geometry, date, visibility, then none, e.g. a geom field cannot have an
    // invalid date format metadata, but, if it has valid date format metadata, it will be ignored
    val geomFormat = GeomesaAvroGeomFormat.parse(field)
    val geomType = GeomesaAvroGeomType.parse(field)
    val geomSrid = GeomesaAvroGeomSrid.parse(field)
    val geomDefault = GeomesaAvroGeomDefault.parse(field)

    val dateFormat = GeomesaAvroDateFormat.parse(field)
    val dateDefault = GeomesaAvroDateDefault.parse(field)

    val index = GeomesaAvroIndex.parse(field)
    val cardinality = GeomesaAvroCardinality.parse(field)
    val visibility = GeomesaAvroFeatureVisibility.parse(field)

    val properties =
      if (geomFormat.isDefined && geomType.isDefined) {
        GeomProperties(geomFormat.get, geomType.get, geomDefault.getOrElse(false), geomSrid)
      } else if (dateFormat.isDefined) {
        DateProperties(dateFormat.get, dateDefault.getOrElse(false))
      } else if (visibility.contains(true)) {
        VisibilityProperties
      } else {
        NoProperties
      }

    val extra = Map.empty[String, String] ++
      index.map { SimpleFeatureTypes.AttributeOptions.OptIndex -> _ } ++
      cardinality.map { SimpleFeatureTypes.AttributeOptions.OptCardinality -> _ }

    GeomesaAvroFieldMetadata(properties, extra)
  }

  private case class GeomesaAvroFieldMetadata(properties: GeomesaAvroFieldProperties, extra: Map[String, String])

  private sealed trait GeomesaAvroFieldProperties
  private case class GeomProperties(format: String, typ: Class[_ <: Geometry], default: Boolean, srid: Option[String])
    extends GeomesaAvroFieldProperties
  private case class DateProperties(format: String, default: Boolean) extends GeomesaAvroFieldProperties
  private case object VisibilityProperties extends GeomesaAvroFieldProperties
  private case object NoProperties extends GeomesaAvroFieldProperties

  case class UnsupportedAvroTypeException(typeName: String)
    extends IllegalArgumentException(s"Type '$typeName' is not supported for SFT conversion")

  /**
   * An attribute in an Avro [[Schema.Field]] to provide additional information when creating an SFT.
   *
   * @tparam T the type of the value to be parsed from this property
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
  }

  /**
   * A [[GeomesaAvroProperty]] that has a finite set of possible string values.
   *
   * @tparam T the type of the value to be parsed from this property
   */
  trait GeomesaAvroEnumProperty[T] extends GeomesaAvroProperty[T] {
    // case clauses to match the values of the enum and possibly check the field type
    protected val matcher: PartialFunction[(String, Schema.Field), T]

    override final def parse(field: Schema.Field): Option[T] = {
      Option(field.getProp(KEY)).map(_.toLowerCase(Locale.ENGLISH)).map { value =>
        matcher.lift.apply((value, field)).getOrElse {
          throw GeomesaAvroProperty.InvalidPropertyValueException(value, KEY)
        }
      }
    }
  }

  /**
   * A [[GeomesaAvroEnumProperty]] that parses to a boolean value.
   */
  trait GeomesaAvroBooleanProperty extends GeomesaAvroEnumProperty[Boolean] {
    final val TRUE: String = "true"
    final val FALSE: String = "false"

    override protected val matcher: PartialFunction[(String, Schema.Field), Boolean] = {
      case (TRUE, _) => true
      case (FALSE, _) => false
    }
  }

  /**
   * A [[GeomesaAvroEnumProperty]] that can be deserialized from a avro record.
   *
   * @tparam T the type of the value to be parsed from this property
   * @tparam K the type of the value to be deserialized from this property
   */
  abstract class GeomesaAvroDeserializableEnumProperty[T, K >: Null: ClassTag] extends GeomesaAvroEnumProperty[T] {
    // case clauses to match the values of the enum and deserialize the data accordingly
    protected val dematcher: PartialFunction[(T, AnyRef), K]

    final def deserialize(record: GenericRecord, fieldName: String): K = {
      val data = record.get(fieldName)
      if (data == null) { return null }
      try {
        parse(record.getSchema.getField(fieldName)).map { value =>
          dematcher.lift.apply((value, data)).getOrElse {
            throw GeomesaAvroProperty.InvalidPropertyValueException(value.toString, KEY)
          }
        }.getOrElse {
          throw GeomesaAvroDeserializableEnumProperty.MissingPropertyValueException[K](fieldName, KEY)
        }
      } catch {
        case ex: Exception => throw GeomesaAvroDeserializableEnumProperty.DeserializationException[K](fieldName, ex)
      }
    }
  }

  object GeomesaAvroDeserializableEnumProperty {
    final case class MissingPropertyValueException[K: ClassTag](fieldName: String, key: String)
      extends RuntimeException(s"Cannot process field '$fieldName' for type '${classTag[K].runtimeClass.getName}' " +
        s"because key '$key' is missing")

    final case class DeserializationException[K: ClassTag](fieldName: String, t: Throwable)
      extends RuntimeException(s"Cannot deserialize field '$fieldName' into a '${classTag[K].runtimeClass.getName}': " +
        s"${t.getMessage}")
  }

  /**
   * Indicates that this field should be interpreted as a [[Geometry]], with one of the formats specified below.
   */
  object GeomesaAvroGeomFormat extends GeomesaAvroDeserializableEnumProperty[String, Geometry] {
    override val KEY: String = "geomesa.geom.format"

    /**
     * Well-Known Text representation as a [[String]]
     */
    val WKT: String = "wkt"
    /**
     * Well-Known Bytes representation as an [[Array]] of [[Byte]]s
     */
    val WKB: String = "wkb"

    override protected val matcher: PartialFunction[(String, Schema.Field), String] = {
      case (WKT, field) => assertFieldType(field, Schema.Type.STRING); WKT
      case (WKB, field) => assertFieldType(field, Schema.Type.BYTES); WKB
    }

    override protected val dematcher: PartialFunction[(String, AnyRef), Geometry] = {
      case (WKT, data) => WKTUtils.read(data.toString)
      case (WKB, data) => WKBUtils.read(data.asInstanceOf[ByteBuffer].array())
    }
  }

  /**
   * Indicates that this field represents a [[Geometry]], with one of the types specified below.
   */
  object GeomesaAvroGeomType extends GeomesaAvroEnumProperty[Class[_ <: Geometry]] {
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

    override protected val matcher: PartialFunction[(String, Schema.Field), Class[_ <: Geometry]] = {
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
   * Indicates that this field should be interpreted as the default [[Geometry]] for this [[SimpleFeatureType]].
   */
  object GeomesaAvroGeomDefault extends GeomesaAvroBooleanProperty {
    override val KEY: String = "geomesa.geom.default"
  }

  /**
   * Indicates that this field represents a [[Geometry]] with one of the spatial reference identifiers (SRID)
   * specified below.
   */
  object GeomesaAvroGeomSrid extends GeomesaAvroEnumProperty[String] {
    override val KEY: String = "geomesa.geom.srid"

    /**
     * Longitude/latitude coordinate system on the WGS84 ellipsoid
     */
    val EPSG_4326: String = "4326"

    override protected val matcher: PartialFunction[(String, Schema.Field), String] = {
      case (EPSG_4326, _) => EPSG_4326
    }
  }

  /**
   * Indicates that the field should be interpreted as a [[Date]], with one of the formats specified below.
   */
  object GeomesaAvroDateFormat extends GeomesaAvroDeserializableEnumProperty[String, Date] {
    override val KEY: String = "geomesa.date.format"

    /**
     * Milliseconds since the Unix epoch as a [[Long]]
     */
    val EPOCH_MILLIS: String = "epoch-millis"
    /**
     * A [[String]] with date format "yyyy-MM-dd"
     */
    val ISO_DATE: String = "iso-date"
    /**
     * A [[String]] with date format "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"
     */
    val ISO_INSTANT: String = "iso-instant"
    /**
     * A [[String]] with date format "yyyy-MM-dd'T'HH:mm:ssZZ"
     */
    val ISO_INSTANT_NO_MILLIS: String = "iso-instant-no-millis"

    override protected val matcher: PartialFunction[(String, Schema.Field), String] = {
      case (EPOCH_MILLIS, field) => assertFieldType(field, Schema.Type.LONG); EPOCH_MILLIS
      case (ISO_DATE, field) => assertFieldType(field, Schema.Type.STRING); ISO_DATE
      case (ISO_INSTANT, field) => assertFieldType(field, Schema.Type.STRING); ISO_INSTANT
      case (ISO_INSTANT_NO_MILLIS, field) => assertFieldType(field, Schema.Type.STRING); ISO_INSTANT_NO_MILLIS
    }

    override protected val dematcher: PartialFunction[(String, AnyRef), Date] = {
      case (EPOCH_MILLIS, data) => new Date(data.asInstanceOf[java.lang.Long])
      case (ISO_DATE, data) => ISODateTimeFormat.date().parseDateTime(data.toString).toDate
      case (ISO_INSTANT, data) => ISODateTimeFormat.dateTime().parseDateTime(data.toString).toDate
      case (ISO_INSTANT_NO_MILLIS, data) => ISODateTimeFormat.dateTimeNoMillis().parseDateTime(data.toString).toDate
    }
  }

  /**
   * Indicates that this field should be interpreted as the default [[Date]] for this [[SimpleFeatureType]].
   */
  object GeomesaAvroDateDefault extends GeomesaAvroBooleanProperty {
    override val KEY: String = "geomesa.date.default"
  }

  /**
   * Indicates that this field should be indexed by GeoMesa.
   */
  object GeomesaAvroIndex extends GeomesaAvroEnumProperty[String] {
    override val KEY: String = "geomesa.index"

    /**
     * Index this attribute
     */
    val TRUE: String = "true"
    /**
     * Do not index this attribute
     */
    val FALSE: String = "false"
    /**
     * Index the full [[SimpleFeature]]
     */
    val FULL: String = "full"

    override protected val matcher: PartialFunction[(String, Schema.Field), String] = {
      case (TRUE, _) => TRUE
      case (FALSE, _) => FALSE
      case (FULL, _) => FULL
    }
  }

  /**
   * Specifies a cardinality hint for this attribute, for use by GeoMesa in query planning.
   */
  object GeomesaAvroCardinality extends GeomesaAvroEnumProperty[String] {
    override val KEY: String = "geomesa.cardinality"

    /**
     * Prioritize this attribute index
     */
    val HIGH: String = "high"
    /**
     * De-prioritize this attribute index
     */
    val LOW: String = "low"

    override protected val matcher: PartialFunction[(String, Schema.Field), String] = {
      case (HIGH, _) => HIGH
      case (LOW, _) => LOW
    }
  }

  /**
   * Specifies the visibility attribute for features of this [[SimpleFeatureType]].
   */
  object GeomesaAvroFeatureVisibility extends GeomesaAvroBooleanProperty {
    override val KEY: String = "geomesa.avro.visibility.field"

    override protected val matcher: PartialFunction[(String, Schema.Field), Boolean] = {
      case (TRUE, field) => assertFieldType(field, Schema.Type.STRING); true
      case (FALSE, field) => assertFieldType(field, Schema.Type.STRING); false
    }
  }
}
