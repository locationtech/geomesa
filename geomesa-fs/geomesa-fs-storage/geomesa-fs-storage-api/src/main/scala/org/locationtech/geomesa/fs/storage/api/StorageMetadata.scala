/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api

import com.google.gson._
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{Partition, StorageFile}
import org.locationtech.jts.geom.Envelope

import java.io.Closeable
import java.lang.reflect.Type
import scala.util.control.NonFatal

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
   * @param value value
   */
  def set(key: String, value: String): Unit = throw new UnsupportedOperationException()
}

object StorageMetadata {

  implicit val StorageFileOrdering: Ordering[StorageFile] = Ordering.by[StorageFile, Long](_.timestamp).reverse

  private val gson = JsonSerializers.register(new GsonBuilder()).disableHtmlEscaping().create()

  /**
   * Holds a storage file
   *
   * @param file file name (relative to the root path)
   * @param partition list of partitions that the file belongs to
   * @param count number of entries in the file
   * @param action type of file (append, modify, delete)
   * @param spatialBounds known bounds, if any, keyed by feature type attribute number
   * @param attributeBounds known bounds, if any, keyed by feature type attribute number
   * @param sort sort fields for the file, if any, as feature type attribute number
   * @param timestamp timestamp for the file
   */
  case class StorageFile(
      file: String,
      partition: Partition,
      count: Long,
      action: StorageFileAction.StorageFileAction = StorageFileAction.Append,
      spatialBounds: Seq[SpatialBounds] = Seq.empty,
      attributeBounds: Seq[AttributeBounds] = Seq.empty,
      sort: Seq[Int] = Seq.empty,
      timestamp: Long = System.currentTimeMillis(),
    )

  /**
   * A partition
   *
   * @param values set of dimensions that make up the partition
   */
  case class Partition(values: Set[PartitionKey]) {
    override lazy val toString: String = gson.toJson(this)
  }

  object Partition {

    val None: Partition = Partition(Set.empty[PartitionKey])

    def apply(encoded: String): Partition = {
      try { gson.fromJson(encoded, classOf[Partition]) } catch {
        case NonFatal(e) => throw new RuntimeException(s"Invalid partition json: $encoded", e)
      }
    }
  }

  /**
   * A partition tag
   *
   * @param name partition scheme
   * @param value partition value
   */
  case class PartitionKey(name: String, value: String) {
    override lazy val toString: String = gson.toJson(this)
  }

  object PartitionKey {
    def apply(encoded: String): PartitionKey = {
      try { gson.fromJson(encoded, classOf[PartitionKey]) } catch {
        case NonFatal(e) => throw new RuntimeException(s"Invalid partition key json: $encoded", e)
      }
    }
  }

  /**
    * Action related to a storage file
    */
  object StorageFileAction extends Enumeration {
    type StorageFileAction = Value
    val Append, Modify, Delete = Value
  }

  /**
   * Immutable representation of an envelope
   *
   * Note that conversions to/from 'null' envelopes should be handled carefully, as envelopes are considered
   * null if xmin > xmax, however, when instantiating an envelope it will re-order the coordinates:
   *
   * {{{
   *   val env = new Envelope()
   *   val copy = new Envelope(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
   *   copy == env // false
   * }}}
   *
   * Thus, ensure that 'null' envelopes are converted to `None` and not directly to a bounds object. See
   * `PartitionBounds.apply`
   *
   * @param attribute index of the attribute being bounded
   * @param xmin min x dimension
   * @param ymin min y dimension
   * @param xmax max x dimension
   * @param ymax max y dimension
   */
  case class SpatialBounds(attribute: Int, xmin: Double, ymin: Double, xmax: Double, ymax: Double) {

    /**
     * Calculate the minimal bounds encompassing both bounds
     *
     * @param b other bounds
     * @return
     */
    def +(b: SpatialBounds): SpatialBounds = {
      require(attribute == b.attribute, "Trying to merge bounds from different attributes")
      SpatialBounds(attribute, math.min(xmin, b.xmin), math.min(ymin, b.ymin), math.max(xmax, b.xmax), math.max(ymax, b.ymax))
    }

    /**
     * Convert to a mutable envelope
     *
     * @return
     */
    def envelope: Envelope = new Envelope(xmin, xmax, ymin, ymax)
  }

  object SpatialBounds {

    /**
     * Converts an envelope to a bounds, handling 'null' (empty) envelopes
     *
     * @param env envelope
     * @return
     */
    def apply(attribute: Int, env: Envelope): Option[SpatialBounds] =
      if (env.isNull) { None } else { Some(SpatialBounds(attribute, env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)) }
  }

  /**
   * Bounds for an attribute
   *
   * @param attribute index of the attribute in the feature type
   * @param lower lower bound (lexicoded)
   * @param upper upper bound (lexicoded)
   */
  case class AttributeBounds(attribute: Int, lower: String, upper: String)

  object JsonSerializers {

    def register(builder: GsonBuilder): GsonBuilder =
      builder.registerTypeAdapter(classOf[Partition], JsonSerializers.PartitionSerializer)
        .registerTypeAdapter(classOf[PartitionKey], JsonSerializers.PartitionKeySerializer)
        .registerTypeAdapter(classOf[StorageFile], StorageFileSerializer)

    /**
     * Json serializer for partitions
     */
    private object PartitionSerializer extends JsonSerializer[Partition] with JsonDeserializer[Partition] {
      override def serialize(src: Partition, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
        val array = new JsonArray(src.values.size)
        src.values.toSeq.sortBy(k => (k.name, k.value)).foreach { value =>
          array.add(context.serialize(value))
        }
        array
      }

      override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Partition = {
        val array = json.getAsJsonArray
        val values = Set.newBuilder[PartitionKey]
        var i = 0
        while (i < array.size()) {
          values += context.deserialize(array.get(i), classOf[PartitionKey])
          i += 1
        }
        Partition(values.result())
      }
    }

    /**
     * Json serializer for partition keys
     */
    private object PartitionKeySerializer extends JsonSerializer[PartitionKey] with JsonDeserializer[PartitionKey] {
      override def serialize(src: PartitionKey, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
        val obj = new JsonObject()
        obj.addProperty("name", src.name)
        obj.addProperty("value", src.value)
        obj
      }

      override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): PartitionKey = {
        val obj = json.getAsJsonObject
        val name = obj.getAsJsonPrimitive("name").getAsString
        val value = obj.getAsJsonPrimitive("value").getAsString
        PartitionKey(name, value)
      }
    }

    /**
     * Json serializer for StorageFileAction
     */
    private object StorageFileSerializer extends JsonSerializer[StorageFile] with JsonDeserializer[StorageFile] {

      import scala.collection.JavaConverters._

      override def serialize(src: StorageFile, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
        val obj = new JsonObject()
        obj.addProperty("file", src.file)
        obj.add("partition", context.serialize(src.partition))
        obj.addProperty("count", src.count)
        obj.addProperty("action", src.action.toString)
        val spatialBounds = new JsonArray(src.spatialBounds.size)
        src.spatialBounds.foreach(b => spatialBounds.add(context.serialize(b)))
        obj.add("spatialBounds", spatialBounds)
        val attributeBounds = new JsonArray(src.attributeBounds.size)
        src.attributeBounds.foreach(b => attributeBounds.add(context.serialize(b)))
        obj.add("attributeBounds", attributeBounds)
        val sort = new JsonArray(src.sort.size)
        src.sort.foreach(sort.add(_))
        obj.add("sort", sort)
        obj.addProperty("timestamp", src.timestamp)
        obj
      }

      override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): StorageFile = {
        val obj = json.getAsJsonObject
        val spatialBounds =
          obj.getAsJsonArray("spatialBounds").asList().asScala.map(context.deserialize[SpatialBounds](_, classOf[SpatialBounds])).toSeq
        val attributeBounds =
          obj.getAsJsonArray("attributeBounds").asList().asScala.map(context.deserialize[AttributeBounds](_, classOf[AttributeBounds])).toSeq
        val sort = obj.getAsJsonArray("sort").asList().asScala.map(_.getAsInt).toSeq
        StorageFile(
          obj.getAsJsonPrimitive("file").getAsString,
          context.deserialize(obj.get("partition"), classOf[Partition]),
          obj.getAsJsonPrimitive("count").getAsLong,
          StorageFileAction.withName(obj.getAsJsonPrimitive("action").getAsString),
          spatialBounds,
          attributeBounds,
          sort,
          obj.getAsJsonPrimitive("timestamp").getAsLong,
        )
      }
    }
  }
}
