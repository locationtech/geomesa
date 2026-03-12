/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.Partition

import java.io.Closeable

package object api {

  type CloseableFeatureIterator = Iterator[SimpleFeature] with Closeable

  /**
    * Holder for file system references
    *
    * @param fs file system
    * @param conf configuration
    * @param root root path
    */
  case class FileSystemContext(fs: FileSystem, conf: Configuration, root: Path, namespace: Option[String] = None)

  object FileSystemContext {
    def apply(root: Path, conf: Configuration): FileSystemContext = FileSystemContext(root.getFileSystem(conf), conf, root)
  }

  /**
    * Identifier plus configuration
    *
    * @param name name
    * @param options configuration
    */
  case class NamedOptions(name: String, options: Map[String, String] = Map.empty)

  /**
    * Holder for the metadata defining a storage instance
    *
    * @param sft simple feature type
    * @param partitions partition scheme configuration
    * @param config key-value configurations
    */
  case class Metadata(sft: SimpleFeatureType, partitions: Seq[String], config: Map[String, String]) {
    def encoding: String = config(Metadata.Encoding)
    def targetFileSize: Option[Long] = config.get(Metadata.TargetFileSize).map(_.toLong)
  }

  object Metadata {

    val Encoding       = "encoding"
//    val LeafStorage    = "leaf-storage"
    val TargetFileSize = "target-file-size"

    def apply(
        sft: SimpleFeatureType,
        encoding: String,
        scheme: Seq[String],
        fileSize: Option[Long] = None): Metadata = {
      val config: Map[String, String] =
        Map(Encoding -> encoding/*, LeafStorage -> java.lang.Boolean.toString(leafStorage)*/) ++
            fileSize.map(f => TargetFileSize -> java.lang.Long.toString(f)).toMap
      Metadata(sft, scheme, config)
    }
  }

  trait Compactable {

    /**
     * Compact a partition - merge multiple data files into a single file.
     *
     * Care should be taken with this method. Currently, there is no guarantee for correct behavior if
     * multiple threads or storage instances attempt to compact the same partition simultaneously.
     *
     * @param partition partition to compact, or all partitions
     * @param fileSize approximate target size of files, in bytes
     * @param threads suggested threads to use for file system operations
     */
    def compact(partition: Partition, fileSize: Option[Long] = None, threads: Int = 1): Unit
  }
}
