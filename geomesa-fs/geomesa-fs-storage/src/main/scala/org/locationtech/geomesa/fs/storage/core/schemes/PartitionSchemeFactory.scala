/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.schemes

import org.apache.iceberg.transforms.PartitionSpecVisitor
import org.apache.iceberg.{PartitionField, PartitionSpec, Schema}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.iceberg.SimpleFeatureIcebergSchema

/**
  * Factory for loading partition schemes
  */
trait PartitionSchemeFactory {

  /**
    * Load a partition scheme
    *
    * @param sft simple feature type
    * @param scheme scheme options
    * @return partition scheme
    */
  def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme]

  /**
   * Load a partition scheme
   *
   * @param schema simple feature schema
   * @param spec iceberg partition spec
   * @param field partition field
   * @return partition scheme
   */
  def load(schema: SimpleFeatureIcebergSchema, spec: Schema, field: PartitionField): Option[PartitionScheme]
}

object PartitionSchemeFactory {

  import scala.collection.JavaConverters._

  lazy private val factories =
    Seq(AttributeScheme, DateTimeScheme, HashScheme, XZ2Scheme, Z2Scheme)

  /**
    * Create a partition scheme instance via available factories
    *
    * @param sft simple feature type
    * @param scheme scheme options
    * @return
    */
  def load(sft: SimpleFeatureType, scheme: String): PartitionScheme = {
    factories.toStream.flatMap(_.load(sft, scheme)).headOption.getOrElse {
      throw new IllegalArgumentException(s"No partition scheme factory implementation exists for name " +
        s"'$scheme'. Available factories: ${factories.map(_.getClass.getName).mkString(", ")}")
    }
  }


  /**
   * Create a partition scheme instance via available factories
   *
   * @param schema simple feature schema
   * @param spec iceberg partition spec
   * @return
   */
  def load(schema: SimpleFeatureIcebergSchema, spec: PartitionSpec): Seq[PartitionScheme] = {
    spec.fields().asScala.toSeq.map { field =>
      factories.toStream.flatMap(_.load(schema, spec.schema(), field)).headOption.getOrElse {
        throw new IllegalArgumentException(s"No partition scheme factory implementation exists for field " +
          s"'$field'. Available factories: ${factories.map(_.getClass.getName).mkString(", ")}")
      }
    }
  }

  /**
   * Base class that implements spec visitor and returns None for everything. PartitionFactories
   * can override the transforms that they are interested in when loading partition specs
   */
  private[schemes] class BaseSpecVisitor extends PartitionSpecVisitor[Option[PartitionScheme]]() {
    override def identity(sourceName: String, sourceId: Int): Option[PartitionScheme] = None
    override def bucket(sourceName: String, sourceId: Int, numBuckets: Int): Option[PartitionScheme] = None
    override def truncate(sourceName: String, sourceId: Int, width: Int): Option[PartitionScheme] = None
    override def year(sourceName: String, sourceId: Int): Option[PartitionScheme] = None
    override def month(sourceName: String, sourceId: Int): Option[PartitionScheme] = None
    override def day(sourceName: String, sourceId: Int): Option[PartitionScheme] = None
    override def hour(sourceName: String, sourceId: Int): Option[PartitionScheme] = None
    override def alwaysNull(fieldId: Int, sourceName: String, sourceId: Int): Option[PartitionScheme] = None
    override def unknown(fieldId: Int, sourceName: String, sourceId: Int, transform: String): Option[PartitionScheme] = None
  }
}
