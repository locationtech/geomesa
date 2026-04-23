/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage

import com.google.gson._
import com.typesafe.config.ConfigFactory
import org.geotools.api.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.PrimitiveConversions.ConvertToBoolean
import org.locationtech.geomesa.utils.text.Suffixes.Memory
import pureconfig.generic.semiauto.deriveConvert
import pureconfig.{ConfigConvert, ConfigSource}

import java.io.Closeable
import java.lang.reflect.Type
import java.net.URI
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

package object core {

  type CloseableFeatureIterator = Iterator[SimpleFeature] with Closeable

  val CacheDurationProperty: SystemProperty = SystemProperty("geomesa.fs.file.cache.duration", "15 minutes")
  val FileValidationEnabled: SystemProperty = SystemProperty("geomesa.fs.validate.file", "false")

  private val gson =
    new GsonBuilder()
      .registerTypeAdapter(classOf[PartitionKey], PartitionKey.PartitionKeySerializer)
      .registerTypeAdapter(classOf[Partition], Partition.PartitionSerializer)
      .disableHtmlEscaping()
      .create()

  /**
   * Holder for file system references
   *
   * @param conf configuration
   * @param root root path
   * @param namespace optional feature namespace
   */
  case class FileSystemContext(root: URI, conf: Map[String, String], namespace: Option[String]) {
    require(root.getScheme != null && root.getScheme.nonEmpty, s"Invalid root, no scheme specified: $root")
    require(root.toString.endsWith("/"), s"Invalid root URI, must end with '/': $root")
  }

  object FileSystemContext {

    def create(root: URI, conf: Map[String, String]): FileSystemContext = create(root, conf, None)

    def create(root: URI, conf: Map[String, String], namespace: Option[String]): FileSystemContext = {
      val validatedRoot = {
        val rootWithScheme = if (root.getScheme != null && root.getScheme.nonEmpty) { root } else {
          new URI("file", root.getAuthority, root.getPath, root.getQuery, root.getFragment)
        }
        val rootWithSlash = if (rootWithScheme.toString.endsWith("/")) { rootWithScheme } else { new URI(rootWithScheme.toString + "/") }
        rootWithSlash
      }
      FileSystemContext(validatedRoot, conf, namespace)
    }
  }

  /**
    * Holder for the metadata defining a storage instance
    *
    * @param sft simple feature type
    * @param partitions partition scheme configuration
    * @param config key-value configurations
    */
  case class Metadata(sft: SimpleFeatureType, partitions: Seq[String], config: Map[String, String]) {
    def targetFileSize: Option[Long] = config.get(Metadata.TargetFileSize).map(_.toLong)
  }

  object Metadata {

    val TargetFileSize = "target-file-size"

    def apply(sft: SimpleFeatureType, scheme: Seq[String], fileSize: Option[Long] = None): Metadata = {
      val config: Map[String, String] = fileSize.map(f => TargetFileSize -> java.lang.Long.toString(f)).toMap
      Metadata(sft, scheme, config)
    }
  }


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

    /**
     * Json serializer for partitions
     */
    private[core] object PartitionSerializer extends JsonSerializer[Partition] with JsonDeserializer[Partition] {
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

    /**
     * Json serializer for partition keys
     */
    private[core] object PartitionKeySerializer extends JsonSerializer[PartitionKey] with JsonDeserializer[PartitionKey] {
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
  }

  /**
   * Ranged bounds
   *
   * @param name partition scheme name
   * @param lower lower bound, inclusive
   * @param upper upper bound, exclusive
   */
  case class PartitionRange(name: String, lower: String, upper: String) {

    /**
     * Is the value contained in this bounds
     *
     * @param value partition value
     * @return
     */
    def contains(value: String): Boolean = value >= lower && value < upper

    /**
     * Attempt to merge two bounds. Only overlapping bounds will result in a successful merge. Trying to merge
     * bounds from a different partition scheme is a logical error.
     *
     * @param other bounds to merge
     * @return
     */
    def merge(other: PartitionRange): Option[PartitionRange] = {
      if (lower <= other.lower) {
        if (upper >= other.upper) {
          Some(this)
        } else if (upper >= other.lower) {
          Some(PartitionRange(name, lower, other.upper))
        } else {
          None
        }
      } else if (lower > other.upper) {
        None
      } else if (upper >= other.upper) {
        Some(PartitionRange(name, other.lower, upper))
      } else {
        Some(other)
      }
    }
  }

  private lazy implicit val SchemeOptionsConvert: ConfigConvert[SchemeOptions] = deriveConvert[SchemeOptions]
  private lazy implicit val NamedOptionsConvert: ConfigConvert[NamedOptions] = deriveConvert[NamedOptions]

  object StorageKeys {
    val SchemeKey    = "geomesa.fs.scheme"
    val FileSizeKey  = "geomesa.fs.file-size"
    val ObserversKey = "geomesa.fs.observers"
  }

  object StorageAttributeKeys {
    val Bounds = "fs.bounds"
  }

  /**
   * Implicit methods to set/retrieve storage configuration options in SimpleFeatureType user data
   *
   * @param sft simple feature type
   */
  implicit class RichSimpleFeatureType(val sft: SimpleFeatureType) extends AnyVal {
    import StorageKeys._
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.DefaultDtgField

    import scala.collection.JavaConverters._

    def setScheme(names: String): Unit = sft.getUserData.put(SchemeKey, names)
    def removeScheme(): Option[Seq[String]] = {
      remove(SchemeKey).map { scheme =>
        // back compatible check for old json-serialized schemes
        if (scheme.trim.startsWith("{")) {
          try {
            def result(name: String, options: Map[String, String]): Seq[String] = {
              val opts = options.map { case (k, v) => s"$k=$v" }.mkString(":")
              name.split(",").toSeq.map(n => s"$n:$opts")
            }
            val source = ConfigSource.fromConfig(ConfigFactory.parseString(scheme))
            source.load[SchemeOptions] match {
              case Right(o) => result(o.scheme, o.options)
              case Left(_) =>
                val n = source.loadOrThrow[NamedOptions]
                result(n.name, n.options)
            }
          } catch {
            case NonFatal(e) => throw new RuntimeException(s"Could not parse legacy scheme options: $scheme", e)
          }
        } else {
          scheme.split(",").toSeq
        }
      }
    }

    def setTargetFileSize(size: String): Unit = {
      // validate input
      Memory.bytes(size).failed.foreach(e => throw new IllegalArgumentException("Invalid file size", e))
      sft.getUserData.put(FileSizeKey, size)
    }
    def removeTargetFileSize(): Option[Long] = {
      remove(FileSizeKey).map { s =>
        Memory.bytes(s) match {
          case Success(b) => b
          case Failure(e) => throw new IllegalArgumentException("Invalid file size", e)
        }
      }
    }

    def setObservers(names: Seq[String]): Unit = sft.getUserData.put(ObserversKey, names.mkString(","))
    def getObservers: Seq[String] = {
      val obs = sft.getUserData.get(ObserversKey).asInstanceOf[String]
      if (obs == null || obs.isEmpty) { Seq.empty } else { obs.split(",") }
    }

    def spatialBounds(): Seq[Int] = sft.getAttributeDescriptors.asScala.toSeq.collect {
      case g: GeometryDescriptor if g == sft.getGeometryDescriptor || g.fsBounds() => sft.indexOf(g.getLocalName)
    }

    def nonSpatialBounds(): Seq[Int] = sft.getAttributeDescriptors.asScala.toSeq.collect {
      case d if sft.getUserData.get(DefaultDtgField) == d.getLocalName || (d.fsBounds() && !d.isInstanceOf[GeometryDescriptor]) =>
        sft.indexOf(d.getLocalName)
    }

    private def remove(key: String): Option[String] = Option(sft.getUserData.remove(key).asInstanceOf[String])
  }

  implicit class RichAttributeDescriptor(val ad: AttributeDescriptor) extends AnyVal {
    def fsBounds(): Boolean = {
      val b = ad.getUserData.get(StorageAttributeKeys.Bounds)
      if (b == null) { false } else { Try(ConvertToBoolean.convert(b)).getOrElse(false) }
    }
  }

  // kept around for back compatibility with encoded partition schemes
  private case class SchemeOptions(scheme: String, options: Map[String, String] = Map.empty)
  private case class NamedOptions(name: String, options: Map[String, String] = Map.empty)
}
