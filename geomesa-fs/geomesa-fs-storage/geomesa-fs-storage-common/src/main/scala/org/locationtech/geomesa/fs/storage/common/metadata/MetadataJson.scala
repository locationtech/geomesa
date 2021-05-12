/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common // get pureconfig converters from common package
package metadata

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Options.CreateOpts
import org.apache.hadoop.fs.{CreateFlag, Path}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionMetadata, StorageFile}
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling
import pureconfig.{ConfigSource, ConfigWriter}

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.util.control.NonFatal

/**
  * File storing the connection parameters for a metadata instance
  */
object MetadataJson extends MethodProfiling {

  val MetadataPath = "metadata.json"

  private val cache = new ConcurrentHashMap[String, NamedOptions]()

  /**
    * Read the metadata file at the given root path, if it exists
    *
    * @param context context
    * @return
    */
  def readMetadata(context: FileSystemContext): Option[NamedOptions] = {
    val key = context.root.toUri.toString
    var cached = cache.get(key)
    if (cached == null) {
      // note: we may end up reading more than once, but doing it multiple times will only incur
      // a slight performance penalty but not cause any incorrectness
      // using an atomic operation or cache loader can cause problems, as we sometimes insert into the
      // map during the load, which is not allowed
      val file = new Path(context.root, MetadataPath)
      if (PathCache.exists(context.fc, file)) {
        val config = profile("Loaded metadata configuration") {
          WithClose(new InputStreamReader(context.fc.open(file), StandardCharsets.UTF_8)) { in =>
            ConfigFactory.load(ConfigFactory.parseReader(in, ParseOptions)) // call load to resolve sys props
          }
        }
        if (config.hasPath("name")) {
          cached = profile("Parsed metadata configuration") {
            ConfigSource.fromConfig(config).loadOrThrow[NamedOptions]
          }
          cache.put(key, cached)
        } else {
          context.fc.rename(file, new Path(context.root, s"$MetadataPath.bak"))
          PathCache.invalidate(context.fc, file)
          transitionMetadata(context, config).foreach { meta =>
            cached = meta // will be set in the cache in the transition code
          }
        }
      }
    }
    Option(cached)
  }

  /**
    * Write a metadata file at the given root path
    *
    * @param context context
    * @param metadata metadata configuration
    */
  def writeMetadata(context: FileSystemContext, metadata: NamedOptions): Unit = {
    val file = new Path(context.root, MetadataPath)
    if (PathCache.exists(context.fc, file, reload = true)) {
      throw new IllegalArgumentException(
        s"Trying to create a new storage instance but metadata already exists at '$file'")
    }
    val data = profile("Serialized metadata configuration") {
      ConfigWriter[NamedOptions].to(metadata).render(RenderOptions)
    }
    // remove quotes around substitutions so that they resolve properly
    // this logic relies on the fact that all strings will be quoted, and just puts another quote on
    // either side of the expression (typesafe will concatenate them), i.e. "foo ${bar}" -> "foo "${bar}""
    val interpolated = data.replaceAll("\\$\\{[a-zA-Z0-9_.]+}", "\"$0\"")
    profile("Persisted metadata configuration") {
      WithClose(context.fc.create(file, java.util.EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent)) { out =>
        out.write(interpolated.getBytes(StandardCharsets.UTF_8))
        out.hflush()
        out.hsync()
      }
    }
    val toCache = if (data == interpolated) { metadata } else {
      // reload through ConfigFactory to resolve substitutions
<<<<<<< HEAD
      ConfigSource.fromConfig(ConfigFactory.load(ConfigFactory.parseString(interpolated, ParseOptions)))
          .loadOrThrow[NamedOptions]
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
      pureconfig.loadConfigOrThrow[NamedOptions](
        ConfigFactory.load(ConfigFactory.parseString(interpolated, ParseOptions)))
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
      pureconfig.loadConfigOrThrow[NamedOptions](
        ConfigFactory.load(ConfigFactory.parseString(interpolated, ParseOptions)))
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    }
    cache.put(context.root.toUri.toString, toCache)
    PathCache.register(context.fc, file)
  }


  /**
    * Transition the old single-file metadata.json to the new append-log format
    *
    * @param context file system context
    * @return
    */
  private def transitionMetadata(context: FileSystemContext, config: Config): Option[NamedOptions] = {
    import scala.collection.JavaConverters._

    try {
      val sft = SimpleFeatureTypes.createType(config.getConfig("featureType"), path = None)
      val encoding = config.getString("encoding")
      val scheme = {
        val schemeConfig = config.getConfig("partitionScheme")
        val schemeOpts = schemeConfig.getConfig("options")
        NamedOptions(schemeConfig.getString("scheme"),
          schemeOpts.entrySet().asScala.map(e => e.getKey -> schemeOpts.getString(e.getKey)).toMap)
      }
      val leafStorage = scheme.options.get("leaf-storage").forall(_.toBoolean)
      val meta = Metadata(sft, encoding, scheme, leafStorage)
      val partitionConfig = config.getConfig("partitions")

      val defaults = FileBasedMetadata.LegacyOptions
      WithClose(new FileBasedMetadataFactory().create(context, defaults.options, meta)) { metadata =>
        partitionConfig.root().entrySet().asScala.foreach { e =>
          val name = e.getKey
          val files = partitionConfig.getStringList(name).asScala.map(StorageFile(_, 0L))
          metadata.addPartition(PartitionMetadata(name, files.toSeq, None, 0L))
        }
      }

      Some(defaults)
    } catch {
      case NonFatal(e) => logger.warn("Error transitioning old metadata format: ", e); None
    }
  }
}
