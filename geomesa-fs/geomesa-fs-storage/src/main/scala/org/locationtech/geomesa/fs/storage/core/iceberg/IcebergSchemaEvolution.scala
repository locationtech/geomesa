/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package iceberg

import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.exceptions.AlreadyExistsException
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.types.Types.NestedField
import org.apache.iceberg.types.{Type, TypeUtil}
import org.apache.iceberg.{PartitionSpec, Transaction, UpdateSchema}
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.fs.storage.core.StorageCatalog.CatalogUpdate
import org.locationtech.geomesa.fs.storage.core.parquet.BloomFilterConfig
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.fs.storage.core.schema.ColumnName
import org.locationtech.geomesa.fs.storage.core.schemes.PartitionSchemeFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.{Geometry, Point}

import scala.collection.mutable.ArrayBuffer

/**
 * Schema evolution for iceberg tables.
 *
 * Each change is validated when it is requested, by running it (along with any previously staged changes) in an iceberg
 * transaction that is then discarded, without modifying the table. Any validation errors are thrown when the change is
 * requested, and the invalid change is not staged. Nothing is persisted until `apply` is invoked, at which point all the
 * staged changes are re-run in a single transaction and committed atomically. The table is only held open for the
 * duration of each call, so an update can be safely abandoned without being applied.
 *
 * An update can only be applied once, even if the apply fails - to re-try a failed apply, a new update must be created.
 *
 * The one exception is a table rename, which iceberg does not support as part of a transaction - it is executed
 * immediately after the transaction is committed.
 *
 * @param catalog catalog
 * @param tableId tableId
 */
class IcebergSchemaEvolution(catalog: Catalog, tableId: TableIdentifier) extends CatalogUpdate with LazyLogging {

  import IcebergSchemaEvolution.{attributeProperty, bloomFilterKeys, colsFor}
  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  private val changes = ArrayBuffer.empty[Transaction => Unit]

  private var renameTo: Option[TableIdentifier] = None
  private var applied = false

  override def columns(): CatalogUpdate.ColumnUpdate = IcebergColumnUpdate
  override def table(): CatalogUpdate.TableUpdate = IcebergTableUpdate
  override def metadata(): CatalogUpdate.MetadataUpdate = IcebergMetadataUpdate

  override def apply(): Unit = {
    if (applied) {
      throw new IllegalStateException("Update has already been applied")
    }
    applied = true
    withTransaction(_.commitTransaction())
    // the rename can't be done as part of the transaction
    renameTo.foreach(catalog.renameTable(tableId, _))
  }

  /**
   * Stage a change, after validating it against the current table plus any previously staged changes
   *
   * @param change change to stage - takes a transaction, including any previously staged changes
   * @return
   */
  private def stage(change: Transaction => Unit): CatalogUpdate = {
    // the transaction is discarded without being committed, it's only used for validation
    withTransaction(change)
    changes += change
    this
  }

  /**
   * Run a function against a new transaction containing all the staged changes
   *
   * @param fn function
   */
  private def withTransaction(fn: Transaction => Unit): Unit = {
    WithClose(catalog.loadTable(tableId)) { table =>
      val tx = table.newTransaction()
      changes.foreach(_.apply(tx))
      fn(tx)
    }
  }

  private def descriptor(schema: SimpleFeatureIcebergSchema, attribute: String): AttributeDescriptor = {
    val descriptor = schema.sft.getDescriptor(attribute)
    if (descriptor == null) {
      throw new IllegalArgumentException(
        s"Attribute '$attribute' does not exist in the schema: ${schema.sft.getTypeName} " +
          SimpleFeatureTypes.encodeType(schema.sft))
    }
    descriptor
  }

  private object IcebergColumnUpdate extends CatalogUpdate.ColumnUpdate {

    override def add(attribute: AttributeDescriptor): CatalogUpdate = {
      stage { tx =>
        colsFor(attribute).foldLeft(tx.updateSchema())((update, col) => update.addColumn(col.name(), col.`type`())).commit()
        val (columnSpecProp, columnSpec) = attributeProperty(attribute)
        tx.updateProperties().set(columnSpecProp, columnSpec).commit()
      }
    }

    /**
     * Update an attribute. The only supported type changes are widening (int -> long, etc) and modifying structural types.
     * For structural types, changes are detected based on the field names - fields with the same name will be treated as updates,
     * while fields with different names will be treated as new fields (and missing field names will be dropped).
     *
     * @param update updated attribute
     */
    override def update(update: AttributeDescriptor): CatalogUpdate = {
      stage { tx =>
        val schema = SimpleFeatureIcebergSchema(tx.table())
        val existing = descriptor(schema, update.getLocalName)

        val existingIsGeom = classOf[Geometry].isAssignableFrom(existing.getType.getBinding)
        val updateIsGeom = classOf[Geometry].isAssignableFrom(update.getType.getBinding)

        if (existingIsGeom || updateIsGeom) {
          // we don't need to make any actual column changes, just validate that the op is allowed
          if (!existingIsGeom || !updateIsGeom) {
            throw new UnsupportedOperationException(
              s"Attribute '${existing.getLocalName}' can not be changed from ${existing.getType.getBinding.getSimpleName} " +
                s"to ${update.getType.getBinding.getSimpleName}: geometry attributes can't be converted to or from " +
                "non-geometry types")
          } else if (existing.getType.getBinding != update.getType.getBinding &&
              (existing.getType.getBinding == classOf[Point] || update.getType.getBinding != classOf[Geometry])) {
            throw new UnsupportedOperationException(
              s"Attribute '${existing.getLocalName}' can not be changed from ${existing.getType.getBinding.getSimpleName} " +
                s"to ${update.getType.getBinding.getSimpleName}: the only supported geometry change is widening non-point " +
                "types to mixed Geometry")
          }
          // the stored data is always wkb, so the encoding can't be changed
          update.getEncoding.foreach { encoding =>
            if (GeometryEncoding(encoding) != GeometryEncoding.GeoParquetWkb) {
              throw new UnsupportedOperationException(
                s"Attribute '${existing.getLocalName}' can not be changed to encoding '$encoding': " +
                  "only WKB encoding is supported for geometry types")
            }
          }
        } else {
          // assertion: only geometry types create more than 1 column, so we only need to deal with .head
          val column = IcebergSchemaEvolution.colsFor(update).head
          val updateSchema = tx.updateSchema()
          val existingColumn = schema.schema.findField(column.name())
          val path = FieldPath(existing.getLocalName, column.name())
          checkNullabilityUpdates(path, column.isOptional, existingColumn.isOptional, updateSchema)
          walkTypeUpdates(path, column.`type`(), existingColumn.`type`(), updateSchema)
          updateSchema.commit()
        }

        val (columnSpecProp, columnSpec) = attributeProperty(update)
        tx.updateProperties().set(columnSpecProp, columnSpec).commit()
      }
    }

    private def checkNullabilityUpdates(path: FieldPath, updateOptional: Boolean, existingOptional: Boolean, op: UpdateSchema): Unit = {
      if (updateOptional && !existingOptional) {
        op.makeColumnOptional(path.column)
      } else if (!updateOptional && existingOptional) {
        // existing data may contain nulls, so this isn't safe
        throw path.error("can't be changed from optional to required")
      }
    }

    // note: nested types are compared structurally, as the field ids of the update will not match the existing ids
    // noinspection NoTailRecursionAnnotation
    private def walkTypeUpdates(path: FieldPath, update: Type, existing: Type, op: UpdateSchema): Unit = {
      if (update.isStructType && existing.isStructType) {
        val updateStruct = update.asStructType()
        val existingStruct = existing.asStructType()
        updateStruct.fields().asScala.foreach { field =>
          walkStructFields(path, field, existingStruct.field(field.name()), op)
        }
        existingStruct.fields().asScala.foreach { field =>
          if (updateStruct.field(field.name()) == null) {
            op.deleteColumn(s"${path.column}.${field.name()}")
          }
        }
      } else if (update.isListType && existing.isListType) {
        val updateList = update.asListType()
        val existingList = existing.asListType()
        val element = path.element
        checkNullabilityUpdates(element, updateList.isElementOptional, existingList.isElementOptional, op)
        walkTypeUpdates(element, updateList.elementType(), existingList.elementType(), op)
      } else if (update.isMapType && existing.isMapType) {
        val updateMap = update.asMapType()
        val existingMap = existing.asMapType()
        if (!updateMap.keyType().isPrimitiveType || !existingMap.keyType().isPrimitiveType) {
          // note: geomesa and avro maps always have primitive keys, so this shouldn't come up in practice
          throw path.error("can't be updated, as only maps with primitive keys are supported")
        } else if (updateMap.keyType() != existingMap.keyType()) {
          throw path.error(s"can't change map keys from ${existingMap.keyType()} to ${updateMap.keyType()}")
        }
        val value = path.value
        checkNullabilityUpdates(value, updateMap.isValueOptional, existingMap.isValueOptional, op)
        walkTypeUpdates(value, updateMap.valueType(), existingMap.valueType(), op)
      } else if (update.isPrimitiveType && existing.isPrimitiveType) {
        if (update != existing) {
          if (!TypeUtil.isPromotionAllowed(existing, update.asPrimitiveType())) {
            throw path.error(s"can't be changed from $existing to $update: the only supported primitive type changes " +
              "are widening int to long and float to double")
          }
          op.updateColumn(path.column, update.asPrimitiveType())
        }
      } else if (update != existing) {
        throw path.error(s"can't be changed from $existing to $update")
      }
    }

    private def walkStructFields(parent: FieldPath, update: NestedField, existing: NestedField, op: UpdateSchema): Unit = {
      if (existing == null) {
        op.addColumn(parent.column, update.name(), update.`type`())
      } else {
        val path = parent.field(update.name())
        checkNullabilityUpdates(path, update.isOptional, existing.isOptional, op)
        walkTypeUpdates(path, update.`type`(), existing.`type`(), op)
      }
    }

    override def rename(attribute: String, newName: String): CatalogUpdate = {
      stage { tx =>
        val table = tx.table()
        val schema = SimpleFeatureIcebergSchema(table)
        val existing = descriptor(schema, attribute)
        val renamedDescriptor = SimpleFeatureTypes.renameDescriptor(existing, newName)
        val oldCols = colsFor(existing).map(_.name())
        val newCols = colsFor(renamedDescriptor).map(_.name())
        val oldBloomProps = bloomFilterKeys(attribute).map(k => k -> Option(table.properties().get(k)))
        val propsToAdd = {
          val (columnSpecProp, columnSpec) = attributeProperty(renamedDescriptor)
          val blooms = bloomFilterKeys(newName).zip(oldBloomProps).flatMap { case (newKey, (_, value)) => value.map(newKey -> _) }
          val dtgUserData = {
            val key = s"${IcebergCatalog.UserDataPrefix}${Configs.DefaultDtgField}"
            if (table.properties().get(key) == attribute) { Map(key -> newName) } else { Map.empty }
          }
          Map(columnSpecProp -> columnSpec) ++ blooms ++ dtgUserData
        }
        val propsToRemove = oldBloomProps.map(_._1) :+ IcebergCatalog.columnSpecProperty(oldCols.head)
        val updateSchema = tx.updateSchema()
        oldCols.zip(newCols).foreach { case (old, newName) => updateSchema.renameColumn(old, newName) }
        updateSchema.commit()
        val updateProperties = tx.updateProperties()
        propsToRemove.foreach(updateProperties.remove)
        propsToAdd.foreach { case (k, v) => updateProperties.set(k, v) }
        updateProperties.commit()
      }
    }

    override def remove(attribute: String): CatalogUpdate = {
      stage { tx =>
        val schema = SimpleFeatureIcebergSchema(tx.table())
        val colsToRemove = colsFor(descriptor(schema, attribute)).map(_.name())
        val propsToRemove = bloomFilterKeys(attribute) :+ IcebergCatalog.columnSpecProperty(colsToRemove.head)
        colsToRemove.foldLeft(tx.updateSchema())((update, col) => update.deleteColumn(col)).commit()
        propsToRemove.foldLeft(tx.updateProperties())((update, prop) => update.remove(prop)).commit()
      }
    }

    override def reorder(attribute: String, position: Int): CatalogUpdate = {
      var warn = true // only warn once, as staged changes are run multiple times
      stage { tx =>
        val table = tx.table()
        val schema = SimpleFeatureIcebergSchema(table)
        val sft = schema.sft
        val existing = descriptor(schema, attribute)
        if (position < 0 || position > sft.getAttributeCount - 1) {
          throw new IllegalArgumentException(
            s"Invalid reorder position, not in [0,${sft.getAttributeCount - 1}]: $position")
        } else if (position == sft.indexOf(attribute)) {
          if (warn) {
            logger.warn(s"Ignoring no-op move of attribute $attribute to $position")
            warn = false
          }
        } else {
          val toMove = colsFor(existing)
          if (position == sft.getAttributeCount - 1) {
            val location = table.schema().columns().asScala.last.name()
            toMove.reverse.foldLeft(tx.updateSchema())((update, col) => update.moveAfter(col.name(), location)).commit()
          } else {
            val location = ColumnName.encode(sft.getAttributeDescriptors.asScala.filterNot(_ == existing)(position).getLocalName)
            toMove.foldLeft(tx.updateSchema())((update, col) => update.moveBefore(col.name(), location)).commit()
          }
        }
      }
    }

    /**
     * Path to a (possibly nested) field
     *
     * @param attribute top-level attribute name, used for error messages
     * @param column iceberg column path, used for schema updates
     * @param display user-facing description of the nested field, used for error messages
     */
    private case class FieldPath(attribute: String, column: String, display: Option[String] = None) {
      def field(name: String): FieldPath = FieldPath(attribute, s"$column.$name", Some(display.fold(name)(d => s"$d.$name")))
      def element: FieldPath = FieldPath(attribute, s"$column.element", Some(s"${display.getOrElse("")}[element]"))
      def value: FieldPath = FieldPath(attribute, s"$column.value", Some(s"${display.getOrElse("")}[value]"))
      def error(msg: String): UnsupportedOperationException = {
        val location = display.fold("")(d => s" (nested field '$d')")
        new UnsupportedOperationException(s"Attribute '$attribute'$location $msg")
      }
    }
  }

  private object IcebergTableUpdate extends CatalogUpdate.TableUpdate {

    override def rename(newName: String): CatalogUpdate = {
      val newTableId = TableIdentifier.of(tableId.namespace(), ColumnName.encode(newName))
      stage { tx =>
        if (catalog.tableExists(newTableId)) {
          throw new AlreadyExistsException("Table already exists: %s", newTableId)
        }
        tx.updateProperties().set(IcebergCatalog.TypeNameKey, newName).commit()
      }
      // the actual rename can't be done as part of the transaction, so it's executed after the transaction is committed
      renameTo = Some(newTableId)
      IcebergSchemaEvolution.this
    }

    override def setPartitions(partitions: Seq[String]): CatalogUpdate = {
      stage { tx =>
        val table = tx.table()
        val schema = SimpleFeatureIcebergSchema(table)
        val currentSpec = table.spec().fields().asScala.map(f => (f.sourceId(), f.transform(), f.name()))
        val updatedSpec = {
          val schemes = partitions.map(PartitionSchemeFactory.load(schema.sft, _)).sortBy(_.name)
          val fields = schemes.foldLeft(PartitionSpec.builderFor(schema.schema))((b, m) => m.spec(b)).build().fields().asScala
          fields.map(f => (f.sourceId(), f.transform(), f.name()))
        }

        val updateSpec = tx.updateSpec()
        currentSpec.filterNot(updatedSpec.contains).foreach { case (_, _, name) => updateSpec.removeField(name) }
        updatedSpec.filterNot(currentSpec.contains).foreach { case (sourceId, transform, name) =>
          updateSpec.addField(name, Expressions.transform(schema.schema.findColumnName(sourceId), transform))
        }
        updateSpec.commit()
      }
    }

    override def addBloomFilters(filters: Seq[BloomFilterConfig]): CatalogUpdate = {
      stage { tx =>
        val props = BloomFilterConfig.tableProperties(filters)
        props.foldLeft(tx.updateProperties()) { case (update, (k, v)) => update.set(k, v) }.commit()
      }
    }

    override def removeBloomFilters(attributes: Seq[String]): CatalogUpdate = {
      stage { tx =>
        val props = attributes.flatMap(bloomFilterKeys)
        props.foldLeft(tx.updateProperties())((update, k) => update.remove(k)).commit()
      }
    }
  }

  private object IcebergMetadataUpdate extends CatalogUpdate.MetadataUpdate {

    override def setTargetFileSize(size: Option[Long]): CatalogUpdate = {
      val prop = s"${Metadata.PropertyPrefix}${Metadata.TargetFileSize}"
      stage { tx =>
        val update = tx.updateProperties()
        size match {
          case None => update.remove(prop)
          case Some(s) => update.set(prop, s.toString)
        }
        update.commit()
      }
    }

    override def setUserData(key: String, value: Option[String]): CatalogUpdate = {
      stage { tx =>
        val update = tx.updateProperties()
        value match {
          case None => update.remove(s"${IcebergCatalog.UserDataPrefix}$key")
          case Some(v) => update.set(s"${IcebergCatalog.UserDataPrefix}$key", v)
        }
        update.commit()
      }
    }
  }
}

object IcebergSchemaEvolution {

  import scala.collection.JavaConverters._

  private def attributeProperty(descriptor: AttributeDescriptor): (String, String) = {
    // assertion: the sft is only needed to determine the default geometry field
    IcebergCatalog.sftAttributeProperty(null, descriptor)
  }

  private def colsFor(column: AttributeDescriptor): Seq[NestedField] = {
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.setName("")
    sftBuilder.add(column)
    val sft = sftBuilder.buildFeatureType()
    SimpleFeatureIcebergSchema.create(sft).columns().asScala.drop(2).toSeq // drop fid + vis
  }

  private def bloomFilterKeys(attribute: String): Seq[String] =
    BloomFilterConfig.tableProperties(Seq(BloomFilterConfig(attribute, Some(-1L), Some(-1d)))).keys.toSeq
}
