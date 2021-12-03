package org.locationtech.geomesa.features.avro

import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.Schema
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.jts.geom.{Geometry, LineString, Point, Polygon}
import org.opengis.feature.simple.SimpleFeatureType

import java.util.Date
import scala.collection.convert.ImplicitConversions._
import scala.util.Try

object AvroSimpleFeatureTypeParser extends LazyLogging {
  def schemaToSft(schema: Schema): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder
    builder.setName(schema.getName)
    schema.getFields.foreach { field =>
      val metadata = parseMetadata(field)
      val fieldName = field.name()
      metadata match {
        case GeomMetadata(format, geomType, default) =>
          builder.add(fieldName, geomType)
          builder.userData(sftGeomFormatKey(fieldName), format)
          if (default) {
            builder.setDefaultGeometry(fieldName)
          }
        case DateMetadata(format) =>
          builder.add(fieldName, classOf[Date])
          builder.userData(sftDateFormatKey(fieldName), format)
        case NoMetadata =>
          addFieldToBuilder(builder, field)
      }
    }
    builder.buildFeatureType()
  }

  def addFieldToBuilder(builder: SimpleFeatureTypeBuilder,
                                field: Schema.Field,
                                typeOverride: Option[Schema.Type] = None): Unit = {
    typeOverride.getOrElse(field.schema().getType) match {
      case Schema.Type.STRING  => builder.add(field.name(), classOf[java.lang.String])
      case Schema.Type.BOOLEAN => builder.add(field.name(), classOf[java.lang.Boolean])
      case Schema.Type.INT     => builder.add(field.name(), classOf[java.lang.Integer])
      case Schema.Type.DOUBLE  => builder.add(field.name(), classOf[java.lang.Double])
      case Schema.Type.LONG    => builder.add(field.name(), classOf[java.lang.Long])
      case Schema.Type.FLOAT   => builder.add(field.name(), classOf[java.lang.Float])
      case Schema.Type.BYTES   => builder.add(field.name(), classOf[Array[java.lang.Byte]])
      case Schema.Type.UNION   =>
        val types = field.schema().getTypes.map(_.getType).filter(_ != Schema.Type.NULL).toSet
        println("Types: " + types)
        if (types.size > 1) {
          throw UnsupportedAvroTypeException(types.mkString("[", ", ", "]"))
        } else {
          types.headOption.foreach(typ => addFieldToBuilder(builder, field, Option(typ)))
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

  final def sftGeomFormatKey(name: String): String = s"$name.${GeomesaAvroGeomFormat.KEY}"
  final def sftDateFormatKey(name: String): String = s"$name.${GeomesaAvroDateFormat.KEY}"

  sealed trait GeomesaAvroFieldMetadata
  case class GeomMetadata(format: String, typ: Class[_ <: Geometry], default: Boolean) extends GeomesaAvroFieldMetadata
  case class DateMetadata(format: String) extends GeomesaAvroFieldMetadata
  case object NoMetadata extends GeomesaAvroFieldMetadata

  final case class UnsupportedAvroTypeException(typeName: String)
    extends IllegalArgumentException(s"Type '$typeName' is not supported for SFT conversion")
}

trait GeomesaAvroProperty[T] {
  val KEY: String

  def parse(field: Schema.Field): Option[T]

  protected final def assertFieldType(field: Schema.Field, typ: Schema.Type): Unit = {
    assertFieldTypes(field, Set(typ))
  }

  protected final def assertFieldTypes(field: Schema.Field, types: Set[Schema.Type]): Unit = {
    if (!types.contains(field.schema.getType)) {
      throw GeomesaAvroProperty.InvalidPropertyTypeException(types.map(_.getName).mkString(", "), KEY)
    }
  }
}

object GeomesaAvroProperty {
  final case class InvalidPropertyValueException(value: String, key: String)
    extends IllegalArgumentException(s"Unable to parse value '$value' for property '$key'")

  final case class InvalidPropertyTypeException(typeNames: String, key: String)
    extends IllegalArgumentException(s"Fields with property '$key' must be of type(s): '$typeNames'")
}

object GeomesaAvroGeomDefault extends GeomesaAvroProperty[Boolean] {
  override val KEY = "geomesa.geom.default"

  override def parse(field: Schema.Field): Option[Boolean] = {
    val geomDefault = Option(field.getProp(KEY))
    geomDefault.map(s => {
      assertFieldTypes(field, Set(Schema.Type.STRING, Schema.Type.BYTES))
      Try(s.toBoolean).getOrElse {
        throw GeomesaAvroProperty.InvalidPropertyValueException(s, KEY)
      }
    })
  }
}

trait GeomesaAvroPropertyEnum[T] extends GeomesaAvroProperty[T] {
  val SUPPORTED: Set[String]

  final def supports(value: String): Boolean = SUPPORTED.contains(value.toUpperCase)
  final def supportsOrThrow(value: String): String = {
    if (supports(value)) { value.toUpperCase } else {
      throw GeomesaAvroProperty.InvalidPropertyValueException(value, KEY)
    }
  }
}

object GeomesaAvroGeomFormat extends GeomesaAvroPropertyEnum[String] {
  override val KEY = "geomesa.geom.format"

  val WKT = "WKT"
  val WKB = "WKB"

  override val SUPPORTED = Set(WKT, WKB)

  override def parse(field: Schema.Field): Option[String] = {
    val geomFormat = Option(field.getProp(KEY))
    geomFormat.map(supportsOrThrow(_) match {
      case WKT => assertFieldType(field, Schema.Type.STRING); WKT
      case WKB => assertFieldType(field, Schema.Type.BYTES); WKB
    })
  }
}

object GeomesaAvroGeomType extends GeomesaAvroPropertyEnum[Class[_ <: Geometry]] {
  override val KEY = "geomesa.geom.type"

  val POINT = "POINT"
  val LINESTRING = "LINESTRING"
  val POLYGON = "POLYGON"

  override val SUPPORTED = Set(POINT, LINESTRING, POLYGON)

  override def parse(field: Schema.Field): Option[Class[_ <: Geometry]] = {
    val geomType = Option(field.getProp(KEY))
    geomType.map { typ =>
      val validated = supportsOrThrow(typ)
      assertFieldTypes(field, Set(Schema.Type.STRING, Schema.Type.BYTES))
      validated match {
        case POINT => classOf[Point]
        case LINESTRING => classOf[LineString]
        case POLYGON => classOf[Polygon]
      }
    }
  }
}

private object GeomesaAvroDateFormat extends GeomesaAvroPropertyEnum[String] {
  override val KEY = "geomesa.date.format"

  val ISO8601 = "ISO8601"

  override val SUPPORTED = Set(ISO8601)

  override def parse(field: Schema.Field): Option[String] = {
    val dateFormat = Option(field.getProp(KEY))
    dateFormat.map(supportsOrThrow(_) match {
      case ISO8601 => assertFieldType(field, Schema.Type.STRING); ISO8601
    })
  }
}