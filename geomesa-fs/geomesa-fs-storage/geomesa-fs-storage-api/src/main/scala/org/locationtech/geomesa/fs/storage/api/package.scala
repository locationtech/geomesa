/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage

import java.io.Closeable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

package object api {

  type CloseableFeatureIterator = Iterator[SimpleFeature] with Closeable

  /**
    * Holder for file system references
    *
    * @param fc file context
    * @param conf configuration
    * @param root root path
    */
  case class FileSystemContext(fc: FileContext, conf: Configuration, root: Path, namespace: Option[String] = None)

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
    * @param scheme partition scheme configuration
    * @param config key-value configurations
    */
  case class Metadata(sft: SimpleFeatureType, scheme: NamedOptions, config: Map[String, String]) {
    def encoding: String = config(Metadata.Encoding)
    def leafStorage: Boolean = config(Metadata.LeafStorage).toBoolean
    def targetFileSize: Option[Long] = config.get(Metadata.TargetFileSize).map(_.toLong)
  }

  object Metadata {

    val Encoding       = "encoding"
    val LeafStorage    = "leaf-storage"
    val TargetFileSize = "target-file-size"

    def apply(
        sft: SimpleFeatureType,
        encoding: String,
        scheme: NamedOptions,
        leafStorage: Boolean,
        fileSize: Option[Long] = None): Metadata = {
      val config: Map[String, String] =
        Map(Encoding -> encoding, LeafStorage -> java.lang.Boolean.toString(leafStorage)) ++
            fileSize.map(f => TargetFileSize -> java.lang.Long.toString(f)).toMap
      Metadata(sft, scheme, config)
    }
  }

  /**
    * Case class holding a filter and partitions
    *
    * @param filter filter
    * @param partitions partition names
    */
  case class PartitionFilter(filter: Filter, partitions: Seq[String])

  trait Compactable {

    /**
      * Compact a partition - merge multiple data files into a single file.
      *
      * Care should be taken with this method. Currently, there is no guarantee for correct behavior if
      * multiple threads or storage instances attempt to compact the same partition simultaneously.
      *
      * @param partition partition to compact, or all partitions
      * @param threads suggested threads to use for file system operations
      */
    @deprecated("replaced with compact(Option[String], Option[Long], Int)")
    def compact(partition: Option[String], threads: Int): Unit

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
    // noinspection ScalaDeprecation
    def compact(partition: Option[String], fileSize: Option[Long] = None, threads: Int = 1): Unit = {
      // default impl to prevent API breakage
      compact(partition, threads)
    }
  }
}
