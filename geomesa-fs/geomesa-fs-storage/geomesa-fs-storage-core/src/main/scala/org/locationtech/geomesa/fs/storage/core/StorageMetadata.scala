/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import org.apache.iceberg.DataFile
import org.calrissian.mango.types.{TypeEncoder, TypeRegistry}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.curve.{XZ2SFC, Z2SFC}
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.jts.geom._

import java.io.Closeable
import java.util.HexFormat

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
   * Create a DataFile for a written file. This reads the file to extract metrics.
   *
   * @param filePath file path relative to root
   * @param partition partition
   * @return
   */
  def createDataFile(filePath: String, partition: Partition): DataFile

  /**
   * Add a file
   *
   * @param file file
   */
  def addFile(file: DataFile): Unit = addFiles(Seq(file))

  /**
   * Add files in an atomic operation
   *
   * @param files files
   */
  def addFiles(files: Seq[DataFile]): Unit

  /**
   * Delete a file
   *
   * @param file file
   */
  def removeFile(file: DataFile): Unit

  /**
   * Replace existing files with new ones in an atomic operation
   *
   * @param existing existing files
   * @param replacements replacement files
   */
  def replaceFiles(existing: Seq[DataFile], replacements: Seq[DataFile]): Unit

  /**
   * Get all files
   *
   * @return all files
   */
  // noinspection AccessorLikeMethodIsEmptyParen
  def getFiles(): Seq[DataFile]

  /**
   * Get files for a given partition by name
   *
   * @param partition partition
   * @return files for the given partition
   */
  def getFiles(partition: Partition): Seq[DataFile]

  /**
   * Get files matching a given filter
   *
   * @param filter filter
   */
  def getFiles(filter: Filter): Seq[DataFile]

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

    private val sfc = Z2SFC
    private val factory = new GeometryFactory()
    private val hexFormat = HexFormat.of()

    override val getAlias: String = "z2"

    override def resolves(): Class[Point] = classOf[Point]

    override def encode(value: Point): String = {
      if (value == null) {
        throw new NullPointerException("Null values are not allowed")
      }
      toHex(sfc.index(value.getX, value.getY))
    }

    override def decode(value: String): Point = {
      val (x, y) = sfc.invert(fromHex(value))
      factory.createPoint(new Coordinate(x, y))
    }

    /**
     * Creates a z2 prefix based on a partition key
     *
     * @param value z2 partition value
     * @param bits number of bits used in the z2 partition scheme
     * @return
     */
    def encodePartition(value: String, bits: Int): String = hexFormat.toHexDigits(value.toLong, bits / 4)

    /**
     * Calculate encoded ranges
     *
     * @param queries a sequence of OR'd windows to cover. Each window is in the form (xmin, ymin, xmax, ymax)
     * @param maxRanges rough upper bound on the number of ranges to return
     */
    def ranges(queries: Seq[(Double, Double, Double, Double)], maxRanges: Option[Int] = None): Seq[(String, String)] =
      sfc.ranges(queries, maxRanges = maxRanges).map(r => toHex(r.lower) -> toHex(r.upper))

    // since the first two bits are not used in our z values, drop them so that truncating the hex value aligns with our curve
    private def toHex(z: Long): String = hexFormat.toHexDigits(z << 2)
    private def fromHex(hex: String): Long = HexFormat.fromHexDigitsToLong(hex) >>> 2
  }

  /**
   * Encoder for points
   */
  object XZ2Encoder extends TypeEncoder[Geometry, String] {

    private val sfc = XZ2SFC
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
      sfc.hexEncode(sfc.index(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY))
    }

    override def decode(value: String): Geometry = {
      val (xmin, ymin, xmax, ymax) = sfc.invert(sfc.hexDecode(value))
      val ring = Array(
        new Coordinate(xmin, ymin),
        new Coordinate(xmin, ymax),
        new Coordinate(xmax, ymax),
        new Coordinate(xmax, ymin),
        new Coordinate(xmin, ymin)
      )
      factory.createPolygon(ring)
    }

    /**
     * Calculate encoded ranges
     *
     * @param queries a sequence of OR'd windows to cover. Each window is in the form (xmin, ymin, xmax, ymax)
     * @param maxRanges a rough upper limit on the number of ranges to generate
     */
    def ranges(queries: Seq[(Double, Double, Double, Double)], maxRanges: Option[Int] = None): Seq[(String, String)] =
      sfc.ranges(queries, maxRanges).map(r => sfc.hexEncode(r.lower) -> sfc.hexEncode(r.upper))
  }
}
