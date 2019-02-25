/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Options.CreateOpts
import org.apache.hadoop.fs.{CreateFlag, Path}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.PartitionMetadata
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.fs.storage.common.{ParseOptions, RenderOptions}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling
import pureconfig.ConfigWriter

import scala.util.control.NonFatal

/**
  * File storing the connection parameters for a metadata instance
  */
object MetadataJson extends MethodProfiling {

  private val MetadataPath = "metadata.json"

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
            ConfigFactory.parseReader(in, ParseOptions)
          }
        }
        if (config.hasPath("name")) {
          cached = profile("Parsed metadata configuration") {
            pureconfig.loadConfigOrThrow[NamedOptions](config)
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
    profile("Persisted metadata configuration") {
      WithClose(context.fc.create(file, java.util.EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent)) { out =>
        out.write(data.getBytes(StandardCharsets.UTF_8))
        out.hflush()
        out.hsync()
      }
    }
    cache.put(context.root.toUri.toString, metadata)
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

      WithClose(new FileBasedMetadataFactory().create(context, Map.empty, meta)) { metadata =>
        partitionConfig.root().entrySet().asScala.foreach { e =>
          val name = e.getKey
          val files = partitionConfig.getStringList(name).asScala
          metadata.addPartition(PartitionMetadata(name, files, None, 0L))
        }
      }

      Some(FileBasedMetadata.DefaultOptions)
    } catch {
      case NonFatal(e) => logger.warn("Error transitioning old metadata format: ", e); None
    }
  }
}
