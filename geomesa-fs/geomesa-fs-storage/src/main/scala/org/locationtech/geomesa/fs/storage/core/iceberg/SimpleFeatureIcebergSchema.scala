/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.types.Types._
import org.apache.iceberg.types.{Type, TypeUtil}
import org.apache.iceberg.{MetadataColumns, Schema, Table}
import org.geotools.api.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.expression.PropertyName
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.fs.storage.core.schema.SimpleFeatureSchema.{FeatureIdField, VisibilitiesField}
import org.locationtech.geomesa.fs.storage.core.schema.{BoundingBoxField, ColumnName, SimpleFeatureSchema, ZValueField}
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.geotools.Transform.{ExpressionTransform, PropertyTransform, RenameTransform, Transforms}
import org.locationtech.geomesa.utils.geotools.{ObjectType, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.json.JsonPathParser.PathAttribute

import java.util.concurrent.atomic.AtomicInteger
import scala.util.control.NonFatal

/**
 * Holder for info about a geomesa/iceberg schema
 *
 * @param sft simple feature type represented by this schema
 * @param schema iceberg schema
 */
class SimpleFeatureIcebergSchema private (val sft: SimpleFeatureType, val schema: Schema) extends LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  /**
   * Gets the schema needed for reading a file
   *
   * @param transform query transform definition
   * @param filtered columns that have filters against them
   * @param includeFids include __fid__ column for accurate feature IDs
   * @param includeRowPositions include _file and _pos columns necessary for handling updates/deletes
   * @return
   */
  def read(
      transform: Option[String],
      filtered: Set[String],
      includeFids: Boolean = true,
      includeRowPositions: Boolean = false): SimpleFeatureIcebergSchema = {
    // map of top-level columns to nested fields that we're reading - an empty value means read all nested fields
    val readCols = new java.util.LinkedHashMap[String, Seq[String]]()

    def addReadPath(field: String): Unit = {
      val i = field.indexOf('.')
      if (i == -1) {
        val existing = readCols.get(field)
        if (existing == null || existing.nonEmpty) {
          readCols.put(field, Seq.empty)
        }
      } else {
        val topLevelCol = field.substring(0, i)
        val existing = readCols.get(topLevelCol)
        if (existing == null) {
          readCols.put(topLevelCol, Seq(field.substring(i + 1)))
        } else if (existing.nonEmpty) {
          readCols.put(topLevelCol, (existing ++ Seq(field.substring(i + 1))).distinct)
        }
      }
    }

    if (includeFids) {
      addReadPath(FeatureIdField)
    }
    addReadPath(VisibilitiesField)

    val readSft = transform match {
      case None =>
        sft.getAttributeDescriptors.asScala.foreach(d => addReadPath(ColumnName.encode(d.getLocalName)))
        sft

      case Some(defs) =>
        val readSftBuilder = new SimpleFeatureTypeBuilder()

        Transforms(sft, defs).foreach {
          case t: PropertyTransform =>
            val descriptor = sft.getDescriptor(t.i)
            readSftBuilder.add(descriptor)
            addReadPath(ColumnName.encode(descriptor.getLocalName))

          case t: RenameTransform =>
            val descriptor = sft.getDescriptor(t.i)
            readSftBuilder.add(descriptor)
            addReadPath(ColumnName.encode(descriptor.getLocalName))

          case t: ExpressionTransform =>
            t.expression match {
              case p: PropertyName if p.getPropertyName.startsWith("$") =>
                try {
                  val (descriptor, path) = parseJsonPath(p.getPropertyName, sft)
                  val readPath =
                    if (descriptor.getJsonSchema().isEmpty) {
                      ColumnName.encode(descriptor.getLocalName)
                    } else {
                      val nested = path.elements.takeWhile(_.isInstanceOf[PathAttribute]).map(_.asInstanceOf[PathAttribute].name)
                      if (nested.nonEmpty) {
                        (Seq(ColumnName.encode(descriptor.getLocalName)) ++ nested).mkString(".")
                      } else {
                        ColumnName.encode(descriptor.getLocalName)
                      }
                    }
                  readSftBuilder.add(descriptor)
                  addReadPath(readPath)
                } catch {
                  case NonFatal(e) =>
                    logger.warn("Error parsing json-path for evaluating read columns:", e)
                    FilterHelper.propertyNames(t.expression, sft).map(sft.getDescriptor).foreach { descriptor =>
                      readSftBuilder.add(descriptor)
                      addReadPath(ColumnName.encode(descriptor.getLocalName))
                    }
                }

              case _ =>
                FilterHelper.propertyNames(t.expression, sft).map(sft.getDescriptor).foreach { descriptor =>
                  readSftBuilder.add(descriptor)
                  addReadPath(ColumnName.encode(descriptor.getLocalName))
                }
            }

          case t => throw new UnsupportedOperationException(s"An implementation is missing: ${t.getClass}")
        }

        readSftBuilder.setName(sft.getName)
        val readSft = readSftBuilder.buildFeatureType()
        readSft.getUserData.putAll(sft.getUserData)
        readSft
    }

    filtered.foreach(addReadPath)

    val projection = {
      val fields = readCols.asScala.toSeq.map { case (name, children) =>
        val field = schema.findField(name)
        if (field == null) {
          throw new IllegalArgumentException(s"Unexpected projection: $name")
        }
        if (children.isEmpty) {
          field
        } else {
          val subFields = children.map { dot =>
            val path = s"$name.$dot"
            val subField = schema.findField(path)
            if (subField == null) {
              throw new IllegalArgumentException(s"Unexpected projection: $path")
            }
            dot -> subField
          }
          buildProjectedType(field, subFields)
        }
      }
      if (includeRowPositions) { fields ++ Seq(MetadataColumns.FILE_PATH, MetadataColumns.ROW_POSITION) } else { fields }
    }

    val ids = projection.collectFirst { case f if f.name() == FeatureIdField => Int.box(f.fieldId()) }

    val readSchema = new Schema(projection.asJava, schema.getAliases, ids.toSet.asJava)

    new SimpleFeatureIcebergSchema(readSft, readSchema)
  }

  /**
   * Build a projected field that only contains the subfields that are passed in
   *
   * @param field base field
   * @param subFields subfields to keep, along with the path to the field in dot-notation, may be multiple levels deep
   * @return
   */
  private def buildProjectedType(field: NestedField, subFields: Seq[(String, NestedField)]): NestedField = {
    if (subFields.isEmpty) {
      return field
    }
    // group the requested paths by their first path segment
    val grouped = subFields.groupBy { case (path, _) =>
      val i = path.indexOf('.')
      if (i == -1) { path } else { path.substring(0, i) }
    }
    // walk the original struct fields so we preserve their order, then keep only the requested children
    val projected = field.`type`().asStructType().fields().asScala.flatMap { child =>
      grouped.get(child.name()).map { group =>
        // if the child was requested in full (a leaf path), keep its entire subtree
        if (group.exists { case (path, _) => path.indexOf('.') == -1 }) {
          child
        } else {
          val remaining = group.map { case (path, sub) => path.substring(path.indexOf('.') + 1) -> sub }
          buildProjectedType(child, remaining)
        }
      }
    }
    // preserve the field id, name, doc and optionality, just narrowing the struct type
    NestedField.from(field).ofType(StructType.of(projected.asJava)).build()
  }
}

object SimpleFeatureIcebergSchema extends LazyLogging {

  import SimpleFeatureSchema._
  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  val GeometryEncodingKey = "encoding"

  def apply(table: Table, namespace: Option[String] = None): SimpleFeatureIcebergSchema = {
    val sft = {
      val typeName = table.properties().get("geomesa.sft.name")
      val attributes = table.schema().columns().asScala.flatMap(deriveDescriptor(_, table.properties()))
      val b = new SimpleFeatureTypeBuilder()
      b.setNamespaceURI(namespace.orNull) // important to set this null if not defined so it doesn't default to gml namespace
      b.setName(typeName)
      b.addAll(attributes.asJava)
      attributes.find(d => d.getUserData.get(AttributeOptions.OptDefault) == "true" && d.isInstanceOf[GeometryDescriptor]).foreach { d =>
        b.setDefaultGeometry(d.getLocalName)
      }
      val sft = b.buildFeatureType()
      table.properties().asScala.foreach { case (k, v) =>
        if (k.startsWith(IcebergCatalog.UserDataPrefix)) {
          sft.getUserData.put(k.substring(IcebergCatalog.UserDataPrefix.length), v)
        }
      }
      sft
    }
    new SimpleFeatureIcebergSchema(sft, table.schema())
  }

  private def deriveDescriptor(f: NestedField, properties: java.util.Map[String, String]): Option[AttributeDescriptor] = {
    if (f.name().startsWith(InternalFieldDelimiter) && f.name().endsWith(InternalFieldDelimiter)) { None } else {
      // the spec is a table property - not a column doc
      val key = IcebergCatalog.columnSpecProperty(f.name())
      Option(properties.get(key)) match {
        case None => logger.warn(s"No attribute spec for column '${f.name()}' (expected property '$key')"); None
        case Some(d) =>
          try { Some(SimpleFeatureTypes.createDescriptor(d)) } catch {
            case NonFatal(e) => logger.warn(s"Error parsing column spec as descriptor: $d", e); None
          }
      }
    }
  }

  /**
   * Get a schema based on a simple feature type. Encoding can be configured through `geomesa.parquet.geometries`.
   *
   * Note: this should only be called for creating a new schema, for an existing table the field ids will not be correct
   *
   * @param sft simple feature type
   * @param geometries geometry encoding
   * @return
   */
  def create(sft: SimpleFeatureType, geometries: GeometryEncoding): Schema = {
    val builder = Seq.newBuilder[NestedField]
    // note: we have to use unique field ids, but iceberg will throw them out and regenerate them when creating a table
    val fieldIds = new AtomicInteger(1)
    builder += NestedField.required(FeatureIdField).withId(fieldIds.getAndIncrement()).ofType(StringType.get()).build()
    builder += buildField(VisibilitiesField, fieldIds.getAndIncrement(), null, StringType.get())
    sft.getAttributeDescriptors.asScala.foreach { d =>
      val name = ColumnName(d.getLocalName)
      val objectType = ObjectType.selectType(d)
      // no doc is written: an attribute spec does not fit in a column comment once it carries a
      // structural type definition, so every attribute's spec is a table property - see columnSpecs
      if (objectType.head == ObjectType.GEOMETRY) {
        // TODO supports native geometry encoding
        require(geometries == GeometryEncoding.GeoParquetWkb, "Only WKB encoding is supported for Geometry types")
        // not yet supported in spark or trino: GeometryType.crs84()
        builder += buildField(name.column, fieldIds.getAndIncrement(), null, BinaryType.get())
        builder += BoundingBoxField.icebergSchema(name.column, fieldIds)
        builder += ZValueField.icebergSchema(name.column, objectType(1), fieldIds)
      } else if (objectType.last == ObjectType.JSON) {
        builder +=
          buildField(name.column, fieldIds.getAndIncrement(), null,
            d.getJsonSchema().fold[Type](VariantType.get())(buildStructuralType(_, () => fieldIds.getAndIncrement())))
      } else {
        builder += buildField(name.column, fieldIds.getAndIncrement(), null, getType(objectType, fieldIds))
      }
    }

    new Schema(builder.result().asJava,  java.util.Set.of[Integer](1))
  }

  /**
   * Builds the schema type for an attribute
   *
   * @param name field name
   * @param fieldId field id
   * @param doc field doc
   * @param fieldType field type
   * @return
   */
  private def buildField(name: String, fieldId: Int, doc: String, fieldType: Type): NestedField =
    NestedField.optional(name).withId(fieldId).withDoc(doc).ofType(fieldType).build()

  /**
   * Every column mapped to its full attribute spec, for the table properties that
   * `IcebergCatalog.columnSpecProperty` names.
   *
   * These are the description of the feature type that a written table carries.
   *
   * @param sft simple feature type
   * @return storage column name to attribute spec
   */
  private[iceberg] def columnSpecs(sft: SimpleFeatureType): Map[String, String] = {
    val specs = sft.getAttributeDescriptors.asScala.map { d =>
      ColumnName(d.getLocalName).column -> SimpleFeatureTypes.encodeDescriptor(sft, d)
    }
    specs.toMap
  }

  /**
   * Builds the schema type for an attribute
   *
   * @param bindings object type
   * @param fieldIds ids for nested fields (as needed)
   * @return
   */
  private def getType(bindings: Seq[ObjectType], fieldIds: AtomicInteger): Type = {
    bindings.head match {
      case ObjectType.INT     => IntegerType.get()
      case ObjectType.DOUBLE  => DoubleType.get()
      case ObjectType.LONG    => LongType.get()
      case ObjectType.FLOAT   => FloatType.get()
      case ObjectType.BOOLEAN => BooleanType.get()
      case ObjectType.BYTES   => BinaryType.get()
      case ObjectType.DATE    => TimestampType.withZone()
      case ObjectType.UUID    => UUIDType.get()
      case ObjectType.STRING  => StringType.get()

      case ObjectType.LIST =>
        val subType = buildField("", fieldIds.getAndIncrement(), null, getType(bindings.drop(1), fieldIds))
        ListType.ofRequired(subType.fieldId(), subType.`type`())

      case ObjectType.MAP =>
        val keyType = buildField("", fieldIds.getAndIncrement(), null, getType(bindings.slice(1, 2), fieldIds))
        val valueType = buildField("", fieldIds.getAndIncrement(), null, getType(bindings.slice(2, 3), fieldIds))
        MapType.ofRequired(keyType.fieldId(), valueType.fieldId(), keyType.`type`(), valueType.`type`())

      case binding =>
        throw new UnsupportedOperationException(s"No mapping defined for type: $binding")
    }
  }

  private[core] def buildStructuralType(schemaDef: String, fieldIds: TypeUtil.NextID): Type = {
    val avroSchema = new org.apache.avro.Schema.Parser().parse(schemaDef)
    // wrap in a record so that we handle non-record schemas (arrays, maps), convert, then extract the single field.
    // this mirrors the parquet conversion - a record schema just becomes a struct-typed field.
    val wrapped =
      org.apache.avro.SchemaBuilder.record("wrapper").namespace("tmp")
        .fields().name("value").`type`(avroSchema).noDefault().endRecord()
    val typed = AvroSchemaUtil.convert(wrapped).asStructType().fields().get(0).`type`()
    // top-level scalars are not supported - just use a regular attribute in that case
    if (typed.isPrimitiveType) {
      throw new IllegalArgumentException(
        s"Structural JSON schema must be a record, array, or map, but was a scalar type: $typed")
    }
    // re-stamp the field ids so that they don't overlap our other fields
    TypeUtil.assignFreshIds(typed, fieldIds)
  }
}
