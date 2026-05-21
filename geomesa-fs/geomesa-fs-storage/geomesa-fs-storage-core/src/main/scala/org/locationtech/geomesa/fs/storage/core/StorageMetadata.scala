/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import org.calrissian.mango.types.encoders.lexi.LongEncoder
import org.calrissian.mango.types.{TypeEncoder, TypeRegistry}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.curve.{XZ2SFC, Z2SFC}
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.StorageFile
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.jts.geom._

import java.io.Closeable

/**
  * Metadata interface for managing storage partitions
  */
trait StorageMetadata extends Closeable {

  /**
   * Metadata persistence type
   *
   * @return
   */
  def `type`: String

  /**
    * The schema for SimpleFeatures stored in the file system storage
    *
    * @return schema
    */
  def sft: SimpleFeatureType

  /**
    * The partition scheme(s) used to partition features for storage and querying
    *
    * @return partition schemes
    */
  def schemes: Set[PartitionScheme]

  /**
   * Add a file
   *
   * @param file file
   */
  def addFile(file: StorageFile): Unit

  /**
   * Delete a file
   *
   * @param file file
   */
  def removeFile(file: StorageFile): Unit

  /**
   * Replace existing files with new ones in an atomic operation
   *
   * @param existing existing files
   * @param replacements replacement files
   */
  def replaceFiles(existing: Seq[StorageFile], replacements: Seq[StorageFile]): Unit

  /**
   * Get all files
   *
   * @return all files
   */
  // noinspection AccessorLikeMethodIsEmptyParen
  def getFiles(): Seq[StorageFile]

  /**
   * Get files for a given partition by name
   *
   * @param partition partition
   * @return files for the given partition
   */
  def getFiles(partition: Partition): Seq[StorageFile]

  /**
   * Get files matching a given filter
   *
   * @param filter filter
   */
  def getFiles(filter: Filter): Seq[StorageFile]

  /**
   * Get a previously set key-value pair
   *
   * @param key key
   * @return
   */
  def get(key: String): Option[String] = None

  /**
   * Set a key-value pair
   *
   * @param key key
   * @param value value - may be null
   */
  def set(key: String, value: String): Unit = throw new UnsupportedOperationException()
}

object StorageMetadata {

  implicit val StorageFileOrdering: Ordering[StorageFile] = Ordering.by[StorageFile, Long](_.timestamp).reverse

  val TypeRegistry: TypeRegistry[String] = new TypeRegistry[String](AttributeIndexKey.TypeRegistry, Z2Encoder, XZ2Encoder)

  def typeAlias(binding: Class[_]): String = {
    if (binding == classOf[Point]) {
      Z2Encoder.getAlias
    } else if (classOf[Geometry].isAssignableFrom(binding)) {
      XZ2Encoder.getAlias
    } else {
      TypeRegistry.getClassAlias(binding)
    }
  }

  /**
   * Holds a storage file
   *
   * @param file file name (relative to the root path)
   * @param partition list of partitions that the file belongs to
   * @param count number of entries in the file
   * @param action type of file (append, modify, delete)
   * @param bounds known column bounds, if any, keyed by feature type attribute number
   * @param sort sort fields for the file, if any, as feature type attribute number
   * @param timestamp timestamp for the file
   */
  case class StorageFile(
      file: String,
      partition: Partition,
      count: Long,
      action: StorageFileAction.StorageFileAction = StorageFileAction.Append,
      bounds: Seq[ColumnBounds] = Seq.empty,
      sort: Seq[Int] = Seq.empty,
      timestamp: Long = System.currentTimeMillis(),
    )

  /**
    * Action related to a storage file
    */
  object StorageFileAction extends Enumeration {
    type StorageFileAction = Value
    val Append, Modify, Delete = Value
  }

  /**
   * Bounds for an attribute
   *
   * @param attribute index of the attribute in the feature type
   * @param lower lower bound (lexicoded)
   * @param upper upper bound (lexicoded)
   */
  case class ColumnBounds(attribute: Int, lower: String, upper: String) {
    def decode(sft: SimpleFeatureType): (Any, Any) = {
      val alias = typeAlias(sft.getDescriptor(attribute).getType.getBinding)
      TypeRegistry.decode(alias, lower) -> TypeRegistry.decode(alias, upper)
    }
  }

  /**
   * Encoder for points
   */
  object Z2Encoder extends TypeEncoder[Point, String] {

    val sfc: Z2SFC = Z2SFC

    private val longEncoder = new LongEncoder()
    private val factory = new GeometryFactory()

    override val getAlias: String = "z2"

    override def resolves(): Class[Point] = classOf[Point]

    override def encode(value: Point): String = {
      if (value == null) {
        throw new NullPointerException("Null values are not allowed")
      }
      longEncoder.encode(sfc.index(value.getX, value.getY))
    }

    def encode(z: Long): String = longEncoder.encode(z)

    override def decode(value: String): Point = {
      val (x, y) = sfc.invert(longEncoder.decode(value))
      factory.createPoint(new Coordinate(x, y))
    }
  }

  /**
   * Encoder for points
   */
  object XZ2Encoder extends TypeEncoder[Geometry, String] {

    val sfc: XZ2SFC = XZ2SFC

    private val longEncoder = new LongEncoder()
    private val factory = new GeometryFactory()

    override val getAlias: String = "xz2"

    override def resolves(): Class[Geometry] = classOf[Geometry]

    override def encode(value: Geometry): String = {
      if (value == null) {
        throw new NullPointerException("Null values are not allowed")
      }
      val env = value.getEnvelopeInternal
      if (env.isNull) {
        throw new NullPointerException("Geometry has a null envelope")
      }
      longEncoder.encode(sfc.index(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY))
    }

    def encode(z: Long): String = longEncoder.encode(z)

    override def decode(value: String): Geometry = {
      val (xmin, ymin, xmax, ymax) = sfc.invert(longEncoder.decode(value))
      val ring = Array(
        new Coordinate(xmin, ymin),
        new Coordinate(xmin, ymax),
        new Coordinate(xmax, ymax),
        new Coordinate(xmax, ymin),
        new Coordinate(xmin, ymin)
      )
      factory.createPolygon(ring)
    }
  }
}
