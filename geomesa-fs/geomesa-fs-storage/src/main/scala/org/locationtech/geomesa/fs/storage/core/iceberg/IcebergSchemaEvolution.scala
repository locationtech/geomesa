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
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.transforms.Transform
import org.apache.iceberg.types.Types.NestedField
import org.apache.iceberg.{PartitionSpec, UpdateSchema}
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.fs.storage.core.StorageCatalog.CatalogUpdate
import org.locationtech.geomesa.fs.storage.core.parquet.BloomFilterConfig
import org.locationtech.geomesa.fs.storage.core.schema.ColumnName
import org.locationtech.geomesa.fs.storage.core.schemes.PartitionSchemeFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.{Geometry, Point}

import scala.collection.JavaConverters._

/**
 * Schema evolution for iceberg tables
 *
 * @param catalog catalog
 * @param tableId tableId
 */
class IcebergSchemaEvolution(catalog: Catalog, tableId: TableIdentifier) extends CatalogUpdate with LazyLogging {

  import IcebergSchemaEvolution.{attributeProperty, bloomFilterKeys, colsFor}

  override def columns(): CatalogUpdate.ColumnUpdate = IcebergColumnUpdate
  override def indices(): CatalogUpdate.IndexUpdate = IcebergIndexUpdate
  override def metadata(): CatalogUpdate.MetadataUpdate = IcebergMetadataUpdate

  private object IcebergColumnUpdate extends CatalogUpdate.ColumnUpdate {

    override def add(attribute: AttributeDescriptor): Unit = {
      WithClose(catalog.loadTable(tableId)) { table =>
        val tx = table.newTransaction()
        colsFor(attribute).foldLeft(tx.updateSchema())((update, col) => update.addColumn(col.name(), col.`type`())).commit()
        val (columnSpecProp, columnSpec) = attributeProperty(attribute, default = false)
        tx.updateProperties().set(columnSpecProp, columnSpec).commit()
        tx.commitTransaction()
      }
    }

    /**
     * Update an attribute. The only supported type changes are widening (int -> long, etc) and modifying structural types.
     * For structural types, changes are detected based on the field names - fields with the same name will be treated as updates,
     * while fields with different names will be treated as new fields (and missing field names will be dropped).
     *
     * @param update updated attribute
     */
    override def update(update: AttributeDescriptor): Unit = {
      WithClose(catalog.loadTable(tableId)) { table =>
        val schema = SimpleFeatureIcebergSchema(table)
        val existing = schema.sft.getDescriptor(update.getLocalName)
        if (existing == null) {
          throw new IllegalArgumentException(
            s"Attribute '${update.getLocalName}' does not exist in the schema: ${schema.sft.getTypeName} " +
              SimpleFeatureTypes.encodeType(schema.sft))
        }

        val tx = table.newTransaction()

        if (classOf[Geometry].isAssignableFrom(existing.getType.getBinding)) {
          // we don't need to make any actual column changes, just validate that the op is allowed
          if (existing.getType.getBinding == classOf[Point] || update.getType.getBinding != classOf[Geometry]) {
            throw new UnsupportedOperationException(
              s"Attribute '${existing.getLocalName}' can not be changed from ${existing.getType.getBinding} " +
                s"to ${update.getType.getBinding.getSimpleName}: the only supported geometry change is widening non-point types " +
                s"to mixed Geometry")
          }
        } else {
          // assertion: only geometry types create more than 1 column, so we only need to deal with .head
          val column = IcebergSchemaEvolution.colsFor(update).head
          val updateSchema = tx.updateSchema()
          walkColumnUpdates(null, column, schema.schema.findField(column.name()), updateSchema)
          updateSchema.commit()
        }

        val (columnSpecProp, columnSpec) = attributeProperty(update, existing == schema.sft.getGeometryDescriptor)
        tx.updateProperties().set(columnSpecProp, columnSpec).commit()
        tx.commitTransaction()
      }
    }

    private def walkColumnUpdates(parent: String, update: NestedField, existing: NestedField, op: UpdateSchema): Unit = {
      if (existing == null) {
        op.addColumn(parent, update.name(), update.`type`())
      } else if (update.`type`() != existing.`type`()) {
        val path = Option(parent).fold(update.name())(p => s"$p.${update.name()}")
        if (update.`type`().isStructType && existing.`type`().isStructType) {
          val existingStruct = existing.`type`().asStructType()
          update.`type`().asStructType().fields().asScala.foreach { field =>
            walkColumnUpdates(path, field, existingStruct.field(field.name()), op)
          }
        } else if (update.`type`().isPrimitiveType && existing.`type`().isPrimitiveType) {
          op.updateColumn(path, update.`type`().asPrimitiveType())
        } else {
          throw new UnsupportedOperationException(
            s"Attribute '$path' can't be changed from ${existing.`type`()} to ${update.`type`()}")
        }
      }
    }

    override def rename(attribute: String, newName: String): Unit = {
      WithClose(catalog.loadTable(tableId)) { table =>
        val schema = SimpleFeatureIcebergSchema(table)
        val descriptor = schema.sft.getDescriptor(attribute)
        if (descriptor == null) {
          throw new IllegalArgumentException(
            s"Attribute '$attribute' does not exist in the schema: ${schema.sft.getTypeName} " +
              SimpleFeatureTypes.encodeType(schema.sft))
        }
        val (columnSpecProp, columnSpec) =
          attributeProperty(SimpleFeatureTypes.renameDescriptor(descriptor, newName), descriptor == schema.sft.getGeometryDescriptor)
        val oldCol = ColumnName.encode(attribute)
        val oldBloomProps = bloomFilterKeys(attribute).map(k => k -> table.properties().get(k))
        val propsToAdd =
          Map(columnSpecProp -> columnSpec) ++
            bloomFilterKeys(newName).zip(oldBloomProps).flatMap { case (newKey, (_, value)) => Option(value).map(newKey -> _) }
        val propsToRemove = oldBloomProps.map(_._1) :+ IcebergCatalog.columnSpecProperty(oldCol)
        val tx = table.newTransaction()
        tx.updateSchema().renameColumn(oldCol, ColumnName.encode(newName)).commit()
        val updateProperties = tx.updateProperties()
        propsToRemove.foreach(updateProperties.remove)
        propsToAdd.foreach { case (k, v) => updateProperties.set(k, v) }
        updateProperties.commit()
        tx.commitTransaction()
      }
    }

    override def remove(attribute: String): Unit = {
      val col = ColumnName.encode(attribute)
      val propsToRemove = bloomFilterKeys(attribute) :+ IcebergCatalog.columnSpecProperty(col)
      WithClose(catalog.loadTable(tableId)) { table =>
        val tx = table.newTransaction()
        tx.updateSchema().deleteColumn(col).commit()
        propsToRemove.foldLeft(tx.updateProperties())((update, prop) => update.remove(prop)).commit()
        tx.commitTransaction()
      }
    }

    override def reorder(attribute: String, position: Int): Unit = {
      WithClose(catalog.loadTable(tableId)) { table =>
        val sft = SimpleFeatureIcebergSchema(table).sft
        val descriptor = sft.getDescriptor(attribute)
        if (descriptor == null) {
          throw new IllegalArgumentException(
            s"Attribute '$attribute' does not exist in the schema: ${sft.getTypeName} " +
              SimpleFeatureTypes.encodeType(sft))
        } else if (position < 0 || position > sft.getAttributeCount - 1) {
          throw new IllegalArgumentException(
            s"Invalid reorder position, not in [0,${sft.getAttributeCount - 1}]: $position")
        } else if (position == sft.indexOf(attribute)) {
          logger.warn(s"Ignoring no-op move of attribute $attribute to $position")
          return
        }
        val toMove = colsFor(descriptor)
        if (position == sft.getAttributeCount - 1) {
          val location = table.schema().columns().asScala.last.name()
          toMove.reverse.foldLeft(table.updateSchema())((update, col) => update.moveAfter(col.name(), location)).commit()
        } else {
          val location = ColumnName.encode(sft.getAttributeDescriptors.asScala.filterNot(_ == descriptor)(position).getLocalName)
          toMove.foldLeft(table.updateSchema())((update, col) => update.moveBefore(col.name(), location)).commit()
        }
      }
    }
  }

  private object IcebergIndexUpdate extends CatalogUpdate.IndexUpdate {

    override def setPartitions(partitions: Seq[String]): Unit = {
      WithClose(catalog.loadTable(tableId)) { table =>
        val schema = SimpleFeatureIcebergSchema(table)
        val currentSpec = table.spec().fields().asScala.map(f => (f.sourceId(), f.transform(), f.name()))
        val updatedSpec = {
          val schemes = partitions.map(PartitionSchemeFactory.load(schema.sft, _)).sortBy(_.name)
          val fields = schemes.foldLeft(PartitionSpec.builderFor(schema.schema))((b, m) => m.spec(b)).build().fields().asScala
          fields.map(f => (f.sourceId(), f.transform(), f.name()))
        }

        val updateSpec = table.updateSpec()
        (currentSpec -- updatedSpec).foreach { case (_, _, name) => updateSpec.removeField(name) }
        (updatedSpec -- currentSpec).foreach { case (sourceId, transform, name) =>
          updateSpec.addField(name, Expressions.transform(schema.schema.findColumnName(sourceId), transform))
        }
        updateSpec.commit()
      }
    }

    override def addBloomFilters(filters: Seq[BloomFilterConfig]): Unit = {
      val props = BloomFilterConfig.tableProperties(filters)
      WithClose(catalog.loadTable(tableId)) { table =>
        props.foldLeft(table.updateProperties()) { case (update, (k, v)) => update.set(k, v) }.commit()
      }
    }

    override def removeBloomFilters(attributes: Seq[String]): Unit = {
      val props = attributes.flatMap(bloomFilterKeys)
      WithClose(catalog.loadTable(tableId)) { table =>
        props.foldLeft(table.updateProperties())((update, k) => update.remove(k)).commit()
      }
    }
  }

  private object IcebergMetadataUpdate extends CatalogUpdate.MetadataUpdate {
    override def setTargetFileSize(size: Option[Long]): Unit = {
      val prop = s"${Metadata.PropertyPrefix}${Metadata.TargetFileSize}"
      WithClose(catalog.loadTable(tableId)) { table =>
        val update = table.updateProperties()
        size match {
          case None => update.remove(prop)
          case Some(s) => update.set(prop, s.toString)
        }
        update.commit()
      }
    }
  }
}

object IcebergSchemaEvolution {

  private def attributeProperty(descriptor: AttributeDescriptor, default: Boolean): (String, String) = {
    // assertion: the sft is only needed to determine the default geometry field
    val (k, v) = IcebergCatalog.sftAttributeProperty(null, descriptor)
    if (default) { (k, s"*$v") } else { (k, v) }
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
