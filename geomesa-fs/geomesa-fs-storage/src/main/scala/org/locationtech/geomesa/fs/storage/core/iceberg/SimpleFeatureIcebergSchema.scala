/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg.types.Types._
import org.apache.iceberg.types.{Type, Types}
import org.apache.iceberg.{MetadataColumns, Schema, Table}
import org.geotools.api.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.fs.storage.core.schema.SimpleFeatureSchema.{FeatureIdField, VisibilitiesField}
import org.locationtech.geomesa.fs.storage.core.schema.{BoundingBoxField, ColumnName, SimpleFeatureSchema, ZValueField}
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.geotools.Transform.{ExpressionTransform, PropertyTransform, RenameTransform, Transforms}
import org.locationtech.geomesa.utils.geotools.{ObjectType, SimpleFeatureTypes}

import java.util.concurrent.atomic.AtomicInteger
import scala.util.control.NonFatal

/**
 * Holder for info about a geomesa/iceberg schema
 *
 * @param sft simple feature type represented by this schema
 * @param schema iceberg schema
 */
class SimpleFeatureIcebergSchema private (val sft: SimpleFeatureType, val schema: Schema) {

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
    val baseCols = if (includeFids) { Seq(FeatureIdField, VisibilitiesField) } else { Seq(VisibilitiesField) }
    val (readCols, readSft) = transform match {
      case None =>
        val featureCols = baseCols ++ sft.getAttributeDescriptors.asScala.map(d => ColumnName.encode(d.getLocalName))
        val readCols = featureCols ++ filtered.filterNot(featureCols.contains)
        (readCols, sft)
      case Some(defs) =>
        val fromTransform = Transforms(sft, defs).flatMap {
          case t: PropertyTransform => Seq(sft.getDescriptor(t.i).getLocalName)
          case t: RenameTransform => Seq(sft.getDescriptor(t.i).getLocalName)
          case t: ExpressionTransform => FilterHelper.propertyNames(t.expression, sft)
          case t => throw new UnsupportedOperationException(s"An implementation is missing: ${t.getClass}")
        }
        val attributes = fromTransform.distinct
        val featureCols = baseCols ++ attributes.map(ColumnName.encode)
        val readCols = featureCols ++ filtered.filterNot(featureCols.contains)
        val readSft = {
          val sftBuilder = new SimpleFeatureTypeBuilder()
          attributes.foreach(name => sftBuilder.add(sft.getDescriptor(name)))
          sftBuilder.setName(sft.getName)
          sftBuilder.buildFeatureType()
        }
        readSft.getUserData.putAll(sft.getUserData)
        (readCols, readSft)
    }
    val readSchema = {
      // here we handle the case where  we're only reading some nested fields out of a given top-level field,
      // so that we're not reading things we don't have to
      val parsedCols = new java.util.LinkedHashMap[String, Seq[String]]()
      readCols.foreach { col =>
        val sep = col.indexOf('.')
        if (sep == -1) { parsedCols.put(col, Seq.empty) } else {
          parsedCols.compute(col.substring(0, sep),
            (_, children) => (Option(children).getOrElse(Seq.empty) :+ col.substring(sep + 1)).distinct)
        }
      }
      val projection = parsedCols.asScala.toSeq.map { case (name, children) =>
        val field = schema.findField(name)
        if (field == null) {
          throw new IllegalStateException(s"Unexpected projection: $name")
        }
        if (children.isEmpty || children.size == field.`type`().asStructType().fields().size()) { field } else {
          val subfields = field.`type`().asStructType().fields().asScala.filter(f => children.contains(f.name()))
          Types.NestedField.optional(field.fieldId(), field.name(), Types.StructType.of(subfields.asJava))
        }
      }
      val withRows =
        if (includeRowPositions) { projection ++ Seq(MetadataColumns.FILE_PATH, MetadataColumns.ROW_POSITION) } else { projection }
      val ids = projection.collectFirst { case f if f.name() == FeatureIdField => Int.box(f.fieldId()) }
      new Schema(withRows.asJava, schema.getAliases, ids.toSet.asJava)
    }
    new SimpleFeatureIcebergSchema(readSft, readSchema)
  }
}

object SimpleFeatureIcebergSchema extends LazyLogging {

  import SimpleFeatureSchema._

  import scala.collection.JavaConverters._

  val GeometryEncodingKey = "encoding"

  def apply(table: Table, namespace: Option[String] = None): SimpleFeatureIcebergSchema = {
    val sft = {
      val typeName = table.properties().get("geomesa.sft.name")
      val attributes = table.schema().columns().asScala.flatMap(deriveDescriptor)
      if (attributes.isEmpty) {
        // back compatibility check
        SimpleFeatureTypes.createType(namespace.fold(typeName)(n => s"$n:$typeName"), table.properties().get("geomesa.sft.spec"))
      } else {
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
    }
    new SimpleFeatureIcebergSchema(sft, table.schema())
  }

  private def deriveDescriptor(f: NestedField): Option[AttributeDescriptor] = {
    if (f.name().startsWith(InternalFieldDelimiter) && f.name().endsWith(InternalFieldDelimiter)) { None } else {
      Option(f.doc()).flatMap { d =>
        try { Some(SimpleFeatureTypes.createDescriptor(d)) } catch {
          case NonFatal(e) => logger.warn(s"Error parsing column doc as descriptor: $d", e); None
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
      val doc = SimpleFeatureTypes.encodeDescriptor(sft, d)
      if (objectType.head == ObjectType.STRING) {
        val typed = if (objectType.last == ObjectType.JSON) { VariantType.get() } else { StringType.get() }
        builder += buildField(name.column, fieldIds.getAndIncrement(), doc, typed)
      } else if (objectType.head == ObjectType.GEOMETRY) {
        // TODO supports native geometry encoding
        require(geometries == GeometryEncoding.GeoParquetWkb, "Only WKB encoding is supported for Geometry types")
        val geomDoc = {
          // note: geotools AttributeTypeBuilder shares the user data map - reparse instead so we don't change the original
          val descriptor = SimpleFeatureTypes.createDescriptor(doc)
          descriptor.getUserData.put(GeometryEncodingKey, geometries.toString)
          SimpleFeatureTypes.encodeDescriptor(sft, d)
        }
        // not yet supported in spark or trino: GeometryType.crs84()
        builder += buildField(name.column, fieldIds.getAndIncrement(), geomDoc, BinaryType.get())
        builder += BoundingBoxField.icebergSchema(name.column, fieldIds)
        builder += ZValueField.icebergSchema(name.column, objectType(1), fieldIds)
      } else {
        builder += buildField(name.column, fieldIds.getAndIncrement(), doc, getType(objectType, fieldIds))
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
}
