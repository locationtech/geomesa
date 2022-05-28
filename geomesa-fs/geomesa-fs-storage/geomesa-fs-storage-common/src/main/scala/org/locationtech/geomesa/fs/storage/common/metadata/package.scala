/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.util.Collections

import com.typesafe.config.{Config, ConfigRenderOptions, ConfigValue, ConfigValueFactory}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionBounds, PartitionMetadata, StorageFile, StorageFileAction}
import org.locationtech.geomesa.fs.storage.common.metadata.PartitionAction.PartitionAction
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.Envelope
import org.opengis.feature.simple.SimpleFeatureType
import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.error.{CannotConvert, ConfigReaderFailures}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag

package object metadata {

  import scala.collection.JavaConverters._

  /**
    * Creates a new simple feature type with the namespace in the simple feature type name
    *
    * @param sft simple feature type
    * @param namespace optional namespace
    * @return
    */
  def namespaced(sft: SimpleFeatureType, namespace: Option[String]): SimpleFeatureType =
    namespace.map(ns => SimpleFeatureTypes.renameSft(sft, s"$ns:${sft.getTypeName}")).getOrElse(sft)

  /**
    * Merge configs for a single partition into a single aggregate config
    *
    * @param configs updates for a given partition
    * @return
    */
  def mergePartitionConfigs(configs: Seq[PartitionConfig]): Option[PartitionConfig] = {
    configs.sortBy(_.timestamp).dropWhile(_.action != PartitionAction.Add).reduceLeftOption { (result, update) =>
      update.action match {
        case PartitionAction.Add    => result + update
        case PartitionAction.Remove => result - update
      }
    }
  }

  // case classes for serializing to disk

  case class CompactedConfig(partitions: Seq[PartitionConfig])

  case class PartitionConfig(
      name: String,
      action: PartitionAction,
      files: Seq[StorageFile],
      count: Long,
      envelope: Seq[Double],
      timestamp: Long) {

    def +(other: PartitionConfig): PartitionConfig = {
      require(action == PartitionAction.Add, "Can't aggregate into non-add actions")
      val ts = math.max(timestamp, other.timestamp)
      val env = if (envelope.isEmpty) { other.envelope } else if (other.envelope.isEmpty) { envelope } else {
        val Seq(xmin1, ymin1, xmax1, ymax1) = envelope
        val Seq(xmin2, ymin2, xmax2, ymax2) = other.envelope
        Seq(math.min(xmin1, xmin2), math.min(ymin1, ymin2), math.max(xmax1, xmax2), math.max(ymax1, ymax2))
      }
      PartitionConfig(name, action, files ++ other.files, count + other.count, env, ts)
    }

    def -(other: PartitionConfig): PartitionConfig = {
      require(action == PartitionAction.Add, "Can't aggregate into non-add actions")
      val ts = math.max(timestamp, other.timestamp)
      val names = scala.collection.mutable.Map.empty[String, Int].withDefault(_ => 0)
      other.files.foreach(f => names(f.name) += 1)
      // keep oldest files if there are duplicates
      val fs = files.sortBy(_.timestamp)(Ordering.Long.reverse).filter { f =>
        names(f.name) match {
          case 0 => true
          case i => names.put(f.name, i - 1); false
        }
      }
      PartitionConfig(name, action, fs, math.max(0, count - other.count), envelope, ts)
    }

    def toMetadata: PartitionMetadata = {
      val bounds = envelope match {
        case Nil => None
        case Seq(xmin, ymin, xmax, ymax) => Some(PartitionBounds(xmin, ymin, xmax, ymax))
        case _ => throw new IllegalStateException(s"Invalid envelope: $envelope")
      }
      PartitionMetadata(name, files, bounds, count)
    }
  }

  object PartitionAction extends Enumeration {
    type PartitionAction = Value
    val Add, Remove, Clear = Value
  }

  object EnvelopeConfig {
    def apply(env: Envelope): Seq[Double] =
      if (env == null || env.isNull) { Seq.empty } else { Seq(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY) }
  }

  // pureconfig converters for our case classes

  // noinspection ForwardReference
  sealed trait MetadataConverter {

    def name: String
    def suffix: String

    protected def partitionWriter: ConfigWriter[PartitionConfig]
    protected def compactedWriter: ConfigWriter[CompactedConfig]
    protected def options: ConfigRenderOptions

    private val partitionReader = Derivation.Successful(PartitionReader)
    private val compactedReader = Derivation.Successful(CompactedReader)
    private val partitionCt = ClassTag[PartitionConfig](classOf[PartitionConfig])
    private val compactedCt = ClassTag[CompactedConfig](classOf[CompactedConfig])

    def renderPartition(partition: PartitionConfig): String =
      partitionWriter.to(partition).render(options)
    def parsePartition(config: Config): PartitionConfig =
      pureconfig.loadConfigOrThrow[PartitionConfig](config)(partitionCt, partitionReader)

    def renderCompaction(compaction: Seq[PartitionConfig]): String =
      compactedWriter.to(CompactedConfig(compaction)).render(options)
    def parseCompaction(config: Config): Seq[PartitionConfig] =
      pureconfig.loadConfigOrThrow[CompactedConfig](config)(compactedCt, compactedReader).partitions
  }

  object MetadataConverter {

    private val options = Seq(RenderCompact, RenderPretty)

    def apply(name: String): MetadataConverter = {
      options.find(_.name.equalsIgnoreCase(name)).getOrElse {
        throw new IllegalArgumentException(
          s"Render type '$name' does not exist. Available types: ${options.map(_.name).mkString(", ")}")
      }
    }
  }

  object RenderPretty extends MetadataConverter {
    override val name: String = FileBasedMetadata.Config.RenderPretty
    override val suffix: String = ".json"
    override protected val partitionWriter: ConfigWriter[PartitionConfig] =
      ConfigWriter.fromFunction(writePartitionConfigVerbose)
    override protected val compactedWriter: ConfigWriter[CompactedConfig] =
      ConfigWriter.fromFunction(writeCompactedConfig(partitionWriter))
    override protected val options: ConfigRenderOptions = ConfigRenderOptions.concise().setFormatted(true)
  }

  object RenderCompact extends MetadataConverter {
    override val name: String = FileBasedMetadata.Config.RenderCompact
    override val suffix: String = ".conf"
    override protected val partitionWriter: ConfigWriter[PartitionConfig] =
      ConfigWriter.fromFunction(writePartitionConfigCompact)
    override protected val compactedWriter: ConfigWriter[CompactedConfig] =
      ConfigWriter.fromFunction(writeCompactedConfig(partitionWriter))
    override protected val options: ConfigRenderOptions = ConfigRenderOptions.concise().setJson(false)
  }

  class EnumerationReader[T <: Enumeration](enum: T) extends ConfigReader[T#Value] {
    override def from(cur: ConfigCursor): Result[T#Value] = {
      cur.asString.right.flatMap { s =>
        lazy val reason =
          CannotConvert(s, enum.getClass.getName, s"value $s is not a valid enum: ${enum.values.mkString(", ")}")
        enum.values.find(_.toString.startsWith(s)).asInstanceOf[Option[T#Value]].toRight(cur.failed(reason).left.get)
      }
    }
  }

  private val PartitionReader = ConfigReader.fromCursor(readPartitionConfig)
  private val CompactedReader = ConfigReader.fromCursor(readCompactedConfig)
  private val StorageFileCompactReader = ConfigReader.fromCursor(readStorageFileCompact)
  private val StorageFileVerboseReader = ConfigReader.fromCursor(readStorageFileVerbose)
  private val BoundReader = ConfigReader.fromCursor(readBound)
  private val PartitionActionReader = new EnumerationReader(PartitionAction)
  private val StorageFileActionReader = new EnumerationReader(StorageFileAction)

  private def writeCompactedConfig(delegate: ConfigWriter[PartitionConfig])(compacted: CompactedConfig): ConfigValue =
    ConfigValueFactory.fromMap(Collections.singletonMap("partitions", compacted.partitions.map(delegate.to).asJava))

  private def readCompactedConfig(cur: ConfigCursor): Either[ConfigReaderFailures, CompactedConfig] = {
    for {
      obj  <- cur.asObjectCursor.right
      par  <- obj.atKey("partitions").right.flatMap(_.asListCursor).right
      list <- convertList[PartitionConfig](par)(PartitionReader).right
    } yield {
      CompactedConfig(list)
    }
  }

  private def writePartitionConfigCompact(partition: PartitionConfig): ConfigValue = {
    val list = new java.util.ArrayList[AnyRef](5)
    list.add(partition.action.toString.take(1))
    list.add(Long.box(partition.timestamp))
    list.add(partition.envelope.asJava)
    list.add(Long.box(partition.count))
    list.add(partition.files.map(writeStorageFileCompact).asJava)
    ConfigValueFactory.fromMap(Collections.singletonMap(partition.name, list))
  }

  private def writePartitionConfigVerbose(partition: PartitionConfig): ConfigValue = {
    val map = new java.util.HashMap[String, AnyRef](6)
    map.put("name", partition.name)
    map.put("action", partition.action.toString)
    map.put("files", partition.files.map(writeStorageFileVerbose).asJava)
    map.put("count", Long.box(partition.count))
    map.put("timestamp", Long.box(partition.timestamp))
    if (partition.envelope.nonEmpty) {
      val envelope = new java.util.HashMap[String, AnyRef](4)
      envelope.put("xmin", Double.box(partition.envelope.head))
      envelope.put("ymin", Double.box(partition.envelope(1)))
      envelope.put("xmax", Double.box(partition.envelope(2)))
      envelope.put("ymax", Double.box(partition.envelope(3)))
      map.put("envelope", envelope)
    }
    ConfigValueFactory.fromMap(map)
  }

  private def readPartitionConfig(cur: ConfigCursor): Either[ConfigReaderFailures, PartitionConfig] = {
    val compact = readPartitionConfigCompact(cur)
    if (compact.isRight) { compact } else { readPartitionConfigVerbose(cur).left.flatMap(_ => compact) }
  }

  private def readPartitionConfigCompact(cur: ConfigCursor): Either[ConfigReaderFailures, PartitionConfig] = {
    cur.asObjectCursor.right.flatMap { obj =>
      val name = obj.keys.head
      obj.atKeyOrUndefined(name).asListCursor.right.flatMap { list =>
        if (list.size != 5) {
          cur.failed(CannotConvert(cur.value.toString,
            classOf[PartitionConfig].getSimpleName, s"value ${cur.value} does not have the expected 6 elements"))
        } else {
          for {
            action <- PartitionActionReader.from(list.atIndexOrUndefined(0)).right
            ts     <- list.atIndexOrUndefined(1).asLong.right
            env    <- list.atIndexOrUndefined(2).asListCursor.right.flatMap(readEnvelope).right
            count  <- list.atIndexOrUndefined(3).asLong.right
            files  <- list.atIndexOrUndefined(4).asListCursor.right.flatMap(convertList(_)(StorageFileCompactReader)).right
          } yield {
            PartitionConfig(name, action, files, count, env, ts)
          }
        }
      }
    }
  }

  private def readPartitionConfigVerbose(cur: ConfigCursor): Either[ConfigReaderFailures, PartitionConfig] = {
    def readEnvelope(obj: ConfigObjectCursor): Either[ConfigReaderFailures, Seq[Double]] = {
      obj.atKeyOrUndefined("envelope") match {
        case envelope if envelope.isUndefined => Right(Seq.empty)
        case envelope =>
          for {
            env  <- envelope.asObjectCursor.right
            xmin <- env.atKey("xmin").right.flatMap(_.asDouble).right
            ymin <- env.atKey("ymin").right.flatMap(_.asDouble).right
            xmax <- env.atKey("xmax").right.flatMap(_.asDouble).right
            ymax <- env.atKey("ymax").right.flatMap(_.asDouble).right
          } yield {
            Seq(xmin, ymin, xmax, ymax)
          }
      }
    }

    for {
      obj    <- cur.asObjectCursor.right
      name   <- obj.atKey("name").right.flatMap(_.asString).right
      action <- obj.atKey("action").right.flatMap(PartitionActionReader.from).right
      files  <- obj.atKey("files").right.flatMap(_.asListCursor).right.flatMap(convertList(_)(StorageFileVerboseReader)).right
      count  <- obj.atKey("count").right.flatMap(ConfigReader.longConfigReader.from).right
      bounds <- readEnvelope(obj).right
      ts     <- obj.atKey("timestamp").right.flatMap(ConfigReader.longConfigReader.from).right
    } yield {
      PartitionConfig(name, action, files, count, bounds, ts)
    }
  }

  private def writeStorageFileCompact(file: StorageFile): ConfigValue = {
    val list = new java.util.ArrayList[AnyRef](5)
    list.add(file.name)
    list.add(file.action.toString.take(1))
    list.add(Long.box(file.timestamp))
    if (file.sort.nonEmpty) {
      list.add(file.sort.asJava)
    }
    if (file.bounds.nonEmpty) {
      list.add(file.bounds.map { case (a, f, t) => Seq(a, f, t).asJava }.asJava)
    }
    ConfigValueFactory.fromIterable(list)
  }

  private def writeStorageFileVerbose(file: StorageFile): ConfigValue = {
    val map = new java.util.HashMap[String, AnyRef](5)
    map.put("name", file.name)
    map.put("timestamp", Long.box(file.timestamp))
    map.put("action", file.action.toString)
    if (file.sort.nonEmpty) {
      map.put("sort", file.sort.asJava)
    }
    if (file.bounds.nonEmpty) {
      map.put("bounds", file.bounds.map { case (a, f, t) => Seq(a, f, t).asJava }.asJava)
    }
    ConfigValueFactory.fromMap(map)
  }

  private def readStorageFileCompact(cur: ConfigCursor): Either[ConfigReaderFailures, StorageFile] = {
    cur.asListCursor.right.flatMap { list =>
      if (list.size < 3 || list.size > 5) {
        cur.failed(CannotConvert(cur.value.toString,
          classOf[StorageFile].getSimpleName, s"value ${cur.value} does not have the expected number of elements"))
      } else {
        for {
          name   <- list.atIndexOrUndefined(0).asString.right
          action <- StorageFileActionReader.from(list.atIndexOrUndefined(1)).right
          ts     <- list.atIndexOrUndefined(2).asLong.right
          sort   <- readSort(list.atIndexOrUndefined(3)).right
          bounds <- readBounds(list.atIndexOrUndefined(4)).right
        } yield {
          StorageFile(name, ts, action, sort, bounds)
        }
      }
    }
  }

  private def readStorageFileVerbose(cur: ConfigCursor): Either[ConfigReaderFailures, StorageFile] = {
    val withAction = for {
      obj    <- cur.asObjectCursor.right
      name   <- obj.atKey("name").right.flatMap(_.asString).right
      ts     <- obj.atKey("timestamp").right.flatMap(ConfigReader.longConfigReader.from).right
      action <- obj.atKey("action").right.flatMap(StorageFileActionReader.from).right
      sort   <- readSort(obj.atKeyOrUndefined("sort")).right
      bounds <- readBounds(obj.atKeyOrUndefined("bounds")).right
    } yield {
      StorageFile(name, ts, action, sort, bounds)
    }
    if (withAction.isRight) { withAction } else {
      // note: use 0 for timestamp to sort before any mods
      val sansAction = for { name <- cur.asString.right } yield { StorageFile(name, 0L) }
      sansAction.left.flatMap(_ => withAction) // if failure, replace with original error
    }
  }

  private def readEnvelope(env: ConfigListCursor): Either[ConfigReaderFailures, Seq[Double]] = {
    convertList[Double](env).right.flatMap { coords =>
      if (coords.isEmpty || coords.length == 4) { Right(coords) } else {
        env.failed(CannotConvert(env.value.toString,
          "Seq[Double]", s"value ${env.value} does not have the expected 4 elements"))
      }
    }
  }

  private def readSort(cur: ConfigCursor): Either[ConfigReaderFailures, Seq[Int]] =
    if (cur.isUndefined) { Right(Seq.empty) } else { cur.asListCursor.right.flatMap(convertList[Int]) }

  private def readBounds(cur: ConfigCursor): Either[ConfigReaderFailures, Seq[(Int, String, String)]] =
    if (cur.isUndefined) { Right(Seq.empty) } else { cur.asListCursor.right.flatMap(convertList(_)(BoundReader)) }

  private def readBound(cur: ConfigCursor): Either[ConfigReaderFailures, (Int, String, String)] = {
    cur.asListCursor.right.flatMap { list =>
      if (list.size != 3) {
        cur.failed(CannotConvert(cur.value.toString,
          "Tuple3[Int, String, String]", s"value ${cur.value} does not have 3 elements"))
      } else {
        for {
          attribute <- list.atIndexOrUndefined(0).asInt.right
          from      <- list.atIndexOrUndefined(1).asString.right
          to        <- list.atIndexOrUndefined(2).asString.right
        } yield {
          (attribute, from, to)
        }
      }
    }
  }

  private def convertList[T](
      list: ConfigListCursor)
     (implicit reader: ConfigReader[T]): Either[ConfigReaderFailures, Seq[T]] = {
    val res = Seq.newBuilder[T]
    var err: ConfigReaderFailures = null
    var i = 0
    while (i < list.size) {
      reader.from(list.atIndexOrUndefined(i)) match {
        case Right(success) => res += success
        case Left(failure)  => if (err == null) { err = failure } else { err = err ++ failure }
      }
      i += 1
    }
    if (err == null) { Right(res.result) } else { Left(err) }
  }
}
