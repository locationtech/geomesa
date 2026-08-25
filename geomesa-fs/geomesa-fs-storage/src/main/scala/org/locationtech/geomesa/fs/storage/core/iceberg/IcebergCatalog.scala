/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg.catalog.{Catalog, Namespace, SupportsNamespaces, TableIdentifier}
import org.apache.iceberg.types.Types.NestedField
import org.apache.iceberg.{CatalogUtil, PartitionSpec, RowLevelOperationMode, TableProperties}
import org.geotools.api.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.fs.storage.core.fs.S3ObjectStore
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding.GeoParquetWkb
import org.locationtech.geomesa.fs.storage.core.schema.{ColumnName, SimpleFeatureSchema}
import org.locationtech.geomesa.fs.storage.core.schemes.PartitionSchemeFactory
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorage, Metadata, StorageCatalog, namespaced}
import org.locationtech.geomesa.index.metadata.TableBasedMetadata
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.Closeable
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

/**
 * Catalog implementation backed by iceberg
 *
 * @param config configuration
 */
class IcebergCatalog(config: Map[String, String]) extends StorageCatalog with LazyLogging {

  import IcebergCatalog.{RichCatalog, RichConf, UserDataPrefix}
  import SimpleFeatureSchema.InternalFieldDelimiter
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  val conf: Map[String, String] = S3ObjectStore.s3Configs(config)

  private val expiry = TableBasedMetadata.Expiry.toDuration.get.toMillis

  private val namespace = Namespace.of(ColumnName.encode(conf.required("iceberg.namespace")))
  private val catalog = IcebergCatalog.createCatalog(conf)

  // avoid repeatedly loading tables when getting type names
  private val typeNameCache = Caffeine.newBuilder().expireAfterWrite(expiry, TimeUnit.MILLISECONDS).build(
    new CacheLoader[TableIdentifier, Option[String]]() {
      override def load(id: TableIdentifier): Option[String] = {
        val table = catalog.loadTable(id)
        try { Option(table.properties().get("geomesa.sft.name")) } finally {
          CloseWithLogging(Option(table).collect { case c: Closeable => c })
        }
      }
    }
  )

  override def getTypeNames: Seq[String] = {
    if (catalog.namespaceExists(namespace)) {
      catalog.listTables(namespace).asScala.toSeq.flatMap(id => typeNameCache.get(id))
    } else {
      Seq.empty
    }
  }

  override def load(typeName: String): FileSystemStorage = {
    val table = catalog.loadTable(tableId(typeName))
    val ns = conf.get(StorageCatalog.NamespaceConfigKey)
    val sft = {
      val typeName = table.properties().get("geomesa.sft.name")
      val attributes = table.schema().columns().asScala.flatMap(deriveDescriptor)
      if (attributes.isEmpty) {
        // back compatibility check
        SimpleFeatureTypes.createType(ns.fold(typeName)(n => s"$n:$typeName"), table.properties().get("geomesa.sft.spec"))
      } else {
        val b = new SimpleFeatureTypeBuilder()
        ns.foreach(b.setNamespaceURI)
        b.setName(typeName)
        b.addAll(attributes.asJava)
        attributes.find(d => d.getUserData.get(AttributeOptions.OptDefault) == "true" && d.isInstanceOf[GeometryDescriptor]).foreach { d =>
          b.setDefaultGeometry(d.getLocalName)
        }
        val sft = b.buildFeatureType()
        table.properties().asScala.foreach { case (k, v) =>
          if (k.startsWith(UserDataPrefix)) {
            sft.getUserData.put(k.substring(UserDataPrefix.length), v)
          }
        }
        sft
      }
    }
    val schema = SimpleFeatureIcebergSchema(sft, conf)
    val schemes = PartitionSchemeFactory.load(schema, table.spec())
    FileSystemStorage(table, schemes, schema, conf)
  }

  override def create(sft: SimpleFeatureType, partitions: Seq[String], targetFileSize: Option[Long] = None): FileSystemStorage = {
    val schema = SimpleFeatureIcebergSchema(namespaced(sft, conf.get(StorageCatalog.NamespaceConfigKey)), conf)
    if (schema.geometries != GeoParquetWkb) {
      // TODO supports native geometry encoding
      throw new UnsupportedOperationException(s"Only WKB geometry encoding is supported: ${schema.geometries}")
    }
    // load the partition scheme first in case it fails
    val schemes = partitions.map(PartitionSchemeFactory.load(sft, _)).sortBy(_.name)
    val tableProps = {
      val typeName = Map("geomesa.sft.name" -> sft.getTypeName)
      val userData = {
        val prefixes = sft.getUserDataPrefixes
        sft.getUserData.asScala.collect {
          case (k, v) if v != null && prefixes.exists(k.toString.startsWith) => s"$UserDataPrefix$k" -> v.toString
        }
      }
      val size = targetFileSize.map(s => s"${Metadata.PropertyPrefix}${Metadata.TargetFileSize}" -> s.toString).toMap
      // for back-compatibility
      val spec = Map("geomesa.sft.spec" -> SimpleFeatureTypes.encodeType(sft, includeUserData = true))
      // file format v3 lets us use variant encoding
       val format =
         Map(TableProperties.FORMAT_VERSION -> "3", TableProperties.DELETE_MODE -> RowLevelOperationMode.MERGE_ON_READ.modeName())
      typeName ++ userData ++ size ++ spec ++ format
    }

    val spec = schemes.foldLeft(PartitionSpec.builderFor(schema.schema))((b, m) => m.spec(b)).build()
    catalog.ensureNamespace(namespace)
    val table = catalog.createTable(tableId(sft.getTypeName), schema.schema, spec, null, tableProps.asJava)
    FileSystemStorage(table, schemes, schema, conf)
  }

  override def close(): Unit = catalog.close()

  private def tableId(typeName: String): TableIdentifier = TableIdentifier.of(namespace, ColumnName.encode(typeName))

  private def deriveDescriptor(f: NestedField): Option[AttributeDescriptor] = {
    if (f.name().startsWith(InternalFieldDelimiter) && f.name().endsWith(InternalFieldDelimiter)) { None } else {
      Option(f.doc()).flatMap { d =>
        try { Some(SimpleFeatureTypes.createDescriptor(d)) } catch {
          case NonFatal(e) => logger.warn(s"Error parsing column doc as descriptor: $d", e); None
        }
      }
    }
  }
}

object IcebergCatalog {

  import scala.collection.JavaConverters._

  private val UserDataPrefix = "geomesa.userdata."

  private implicit class RichConf(val conf: Map[String, String]) extends AnyVal {
    def required(k: String): String =
      conf.getOrElse(k, throw new IllegalArgumentException(s"Iceberg catalog requires configuration `$k` to be specified"))
  }

  private implicit class RichCatalog(val catalog: Catalog) extends AnyVal {

    def ensureNamespace(namespace: Namespace): Unit = {
      sn.foreach { supportsNamespace =>
        if (!supportsNamespace.namespaceExists(namespace)) {
          supportsNamespace.createNamespace(namespace)
        }
      }
    }

    def namespaceExists(namespace: Namespace): Boolean = sn.forall(_.namespaceExists(namespace))

    def close(): Unit = CloseWithLogging(Option(catalog).collect { case c: Closeable => c })

    private def sn: Option[SupportsNamespaces] = Option(catalog).collect { case sn: SupportsNamespaces => sn }
  }

  private def createCatalog(conf: Map[String, String]): Catalog = {
    // add some defaults, to reduce boilerplate
    val defaults = Map(
      "io-impl" -> "org.apache.iceberg.aws.s3.S3FileIO",
      "file-format" -> "PARQUET",
    )
    val props = defaults ++ conf
    CatalogUtil.buildIcebergCatalog("geomesa", props.asJava, null)
  }
}
