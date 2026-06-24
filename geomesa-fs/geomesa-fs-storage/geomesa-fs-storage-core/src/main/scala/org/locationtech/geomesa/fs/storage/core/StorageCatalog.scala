/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.catalog.{Catalog, Namespace, SupportsNamespaces, TableIdentifier}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.StorageCatalog.IcebergProps
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding.GeoParquetWkb
import org.locationtech.geomesa.fs.storage.core.parquet.schema.SimpleFeatureParquetSchema
import org.locationtech.geomesa.fs.storage.core.schemes.PartitionSchemeFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.Closeable
import java.util.Locale

class StorageCatalog(val context: FileSystemContext) extends Closeable {

  import scala.collection.JavaConverters._

  private val props = new IcebergProps(context)

  private val namespace = props.namespace
  private val catalog = props.catalog
  private val nsSupport = Option(catalog).collect { case sn: SupportsNamespaces => sn }

  /**
   * Get the feature types known by this factory
   *
   * @return
   */
  def getTypeNames: Seq[String] = {
    if (nsSupport.forall(_.namespaceExists(namespace))) {
      catalog.listTables(namespace).asScala.toSeq.flatMap { id =>
        val table = catalog.loadTable(id)
        try {
          if (table.properties().containsKey("geomesa.sft.name")) { Some(id.name) } else { None }
        } finally {
          CloseWithLogging(Option(table).collect { case c: Closeable => c })
        }
      }
    } else {
      Seq.empty
    }
  }

  /**
   * Load an existing metadata instance by name
   *
   * @param typeName feature type name
   * @return
   */
  def load(typeName: String): FileSystemStorage = {
    val table = catalog.loadTable(tableId(typeName))
    val sft = SimpleFeatureTypes.createType(table.properties().get("geomesa.sft.name"), table.properties().get("geomesa.sft.spec"))
    val schemes = table.properties().get("geomesa.partition.spec").split(",").map(PartitionSchemeFactory.load(sft, _))
    val schema = SimpleFeatureParquetSchema(sft, context.conf)
    FileSystemStorage(context, table, schema, schemes)
  }

  /**
   * Create a metadata instance using the provided options
   *
   * @param sft simple feature type
   * @param partitions storage partitions
   * @param targetFileSize target file size, in bytes
   * @return
   */
  def create(sft: SimpleFeatureType, partitions: Seq[String], targetFileSize: Option[Long] = None): FileSystemStorage = {
    val schema = SimpleFeatureParquetSchema(sft, context.conf)
    if (schema.geometries != GeoParquetWkb) {
      // TODO supports native geometry encoding
      throw new UnsupportedOperationException(s"Only WKB geometry encoding is supported: ${schema.geometries}")
    }
    // load the partition scheme first in case it fails
    val schemes = partitions.map(PartitionSchemeFactory.load(sft, _)).sortBy(_.name)
    val tableProps = Map(
      "geomesa.sft.name" -> sft.getTypeName,
      "geomesa.sft.spec" -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
      "geomesa.partition.spec" -> schemes.map(_.name).mkString(","),
      // file format v3 lets us use native geometries - but it's not yet supported in spark or trino
      // TableProperties.FORMAT_VERSION -> "3"
    ) ++ targetFileSize.map(s => s"${Metadata.PropertyPrefix}${Metadata.TargetFileSize}" -> s.toString).toMap

    nsSupport.foreach { ns =>
      if (!ns.namespaceExists(namespace)) {
        ns.createNamespace(namespace)
      }
    }
    val spec = schemes.foldLeft(PartitionSpec.builderFor(schema.iceberg))((b, m) => m.spec(b)).build()
    val table = catalog.createTable(tableId(sft.getTypeName), schema.iceberg, spec, null, tableProps.asJava)
    FileSystemStorage(context, table, schema, schemes)
  }

  override def close(): Unit = CloseWithLogging(Option(catalog).collect { case c: Closeable => c })

  // TODO valid identifiers vary based on the catalog... this is for glue and not comprehensive
  private def tableId(typeName: String): TableIdentifier =
    TableIdentifier.of(namespace, typeName.toLowerCase(Locale.US).replaceAll("[^a-z0-9]+", "_"))
}

object StorageCatalog {

  private val IcebergPrefix = "iceberg."

  private class IcebergProps(val context: FileSystemContext) extends AnyVal {

    import scala.collection.JavaConverters._

    def namespace: Namespace = Namespace.of(required(s"${IcebergPrefix}namespace"))

    def catalog: Catalog = {
      val impl = required(s"${IcebergPrefix}catalog-impl")
      val catalog = try { Class.forName(impl).getConstructor().newInstance().asInstanceOf[Catalog] } catch {
        case e: Throwable => throw new RuntimeException(s"Could not instantiate catalog class '$impl':", e)
      }
      // map our normal s3 config keys to iceberg ones, so they don't have to be configured 2x
      val s3Configs = context.conf.collect {
        case ("fs.s3.force-path-style", v) => "s3.path-style-access" -> v
        case ("fs.s3.region", v) => "client.region" -> v
        case (k, v) if k.startsWith("fs.s3") => k.substring(3) -> v
      }
      // add some defaults, to reduce boilerplate
      val defaults = Map("io-impl" -> "org.apache.iceberg.aws.s3.S3FileIO",
        "file-format" -> "PARQUET",
        "warehouse" -> context.root.resolve("metadata/").toString
      )
      // TODO Map("parquet.filter.dictionary.enabled" -> "true")
      val props =
        defaults ++ s3Configs ++ context.conf.collect { case (k, v) if k.startsWith(IcebergPrefix) => k.substring(IcebergPrefix.length) -> v }
      catalog.initialize("geomesa", props.asJava)
      catalog
    }

    private def required(k: String): String =
      context.conf.getOrElse(k, throw new IllegalArgumentException(s"Iceberg catalog requires configuration `$k` to be specified"))
  }
}
