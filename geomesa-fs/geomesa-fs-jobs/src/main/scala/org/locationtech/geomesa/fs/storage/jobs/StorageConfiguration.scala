/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.jobs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.fs.storage.core.Partition
import org.locationtech.geomesa.fs.storage.core.schemes.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import java.net.URI

object StorageConfiguration {

  import scala.collection.JavaConverters._

  object Counters {

    val Group    = "org.locationtech.geomesa.jobs.fs"

    val Features = "features"
    val Written  = "written"
    val Failed   = "failed"

    val PartitionPrefix = "p-"

    def partition(name: String): String = PartitionPrefix + name
  }

  val PathKey                  = "geomesa.fs.path"
  val EncodingKey              = "geomesa.fs.encoding"
  val PartitionSchemeKeyPrefix = "geomesa.fs.partition.scheme."
  val PartitionsKeyPrefix      = "geomesa.fs.partitions."
  val FileTypeKey              = "geomesa.fs.output.file-type"
  val FileSizeKey              = "geomesa.fs.output.file-size"
  val SftNameKey               = "geomesa.fs.sft.name"
  val SftSpecKey               = "geomesa.fs.sft.spec"
  val SftReadSpecKey           = "geomesa.fs.sft.read.spec"
  val FilterKey                = "geomesa.fs.filter"
  val TransformSpecKey         = "geomesa.fs.transform.spec"
  val TransformDefinitionKey   = "geomesa.fs.transform.defs"
  val PathActionKey            = "geomesa.fs.path.action"

  def setSft(conf: Configuration, sft: SimpleFeatureType): Unit = {
    val name = Option(sft.getName.getNamespaceURI).map(ns => s"$ns:${sft.getTypeName}").getOrElse(sft.getTypeName)
    conf.set(SftNameKey, name)
    conf.set(SftSpecKey, SimpleFeatureTypes.encodeType(sft, includeUserData = true))
  }
  def getSft(conf: Configuration): SimpleFeatureType =
    SimpleFeatureTypes.createType(conf.get(SftNameKey), conf.get(SftSpecKey))

  def setReadSft(conf: Configuration, sft: SimpleFeatureType): Unit =
    conf.set(SftReadSpecKey, SimpleFeatureTypes.encodeType(sft, includeUserData = true))

  def getSftName(conf: Configuration): String = conf.get(SftNameKey)
  def getSftSpec(conf: Configuration): String = conf.get(SftSpecKey)

  def setRootPath(conf: Configuration, path: URI): Unit = conf.set(PathKey, path.toString)
  def getRootPath(conf: Configuration): URI = new URI(conf.get(PathKey))

  def setPartitionScheme(conf: Configuration, scheme: Seq[PartitionScheme]): Unit = {
    var i = 0
    scheme.foreach { s =>
      conf.set(s"$PartitionSchemeKeyPrefix$i", s.name)
      i += 1
    }
  }
  def getPartitionScheme(conf: Configuration, sft: SimpleFeatureType): Seq[PartitionScheme] =
    conf.getPropsWithPrefix(PartitionSchemeKeyPrefix).asScala.map { case (_, v) => PartitionSchemeFactory.load(sft, v) }.toSeq

  def setPartitions(conf: Configuration, partitions: Seq[Partition]): Unit = {
    var i = 0
    partitions.foreach { p =>
      conf.set(s"$PartitionsKeyPrefix$i", p.toString)
      i += 1
    }
  }
  def getPartitions(conf: Configuration): Seq[Partition] =
    conf.getPropsWithPrefix(PartitionsKeyPrefix).asScala.map { case (_, v) => Partition(v) }.toSeq

  def setTargetFileSize(conf: Configuration, fileSize: Long): Unit = conf.set(FileSizeKey, fileSize.toString)
  def getTargetFileSize(conf: Configuration): Option[Long] = Option(conf.get(FileSizeKey)).map(_.toLong)

  def setFilter(conf: Configuration, filter: Filter): Unit = conf.set(FilterKey, ECQL.toCQL(filter))
  def getFilter(conf: Configuration, sft: SimpleFeatureType): Option[Filter] =
    Option(conf.get(FilterKey)).map(FastFilterFactory.toFilter(sft, _))

  def setTransforms(conf: Configuration, transforms: (String, SimpleFeatureType)): Unit = {
    val (tdefs, tsft) = transforms
    conf.set(TransformDefinitionKey, tdefs)
    conf.set(TransformSpecKey, SimpleFeatureTypes.encodeType(tsft, includeUserData = true))
  }
  def getTransforms(conf: Configuration): Option[(String, SimpleFeatureType)] = {
    for { defs <- Option(conf.get(TransformDefinitionKey)); spec <- Option(conf.get(TransformSpecKey)) } yield {
      (defs, SimpleFeatureTypes.createType("", spec))
    }
  }
}

trait StorageConfiguration {
  def configureOutput(sft: SimpleFeatureType, job: Job): Unit
}
