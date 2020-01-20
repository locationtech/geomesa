/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.jobs

import java.io.{DataInput, DataOutput}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.Job
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.StorageFileAction.StorageFileAction
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{StorageFileAction, StorageFilePath}
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType.FileType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

object StorageConfiguration {

  object Counters {
    val Group    = "org.locationtech.geomesa.jobs.fs"
    val Features = "features"
    val Written  = "written"
    val Failed   = "failed"
  }

  val PathKey                = "geomesa.fs.path"
  val PartitionsKey          = "geomesa.fs.partitions"
  val FileTypeKey            = "geomesa.fs.output.file-type"
  val SftNameKey             = "geomesa.fs.sft.name"
  val SftSpecKey             = "geomesa.fs.sft.spec"
  val FilterKey              = "geomesa.fs.filter"
  val TransformSpecKey       = "geomesa.fs.transform.spec"
  val TransformDefinitionKey = "geomesa.fs.transform.defs"
  val PathActionKey          = "geomesa.fs.path.action"

  def setSft(conf: Configuration, sft: SimpleFeatureType): Unit = {
    val name = Option(sft.getName.getNamespaceURI).map(ns => s"$ns:${sft.getTypeName}").getOrElse(sft.getTypeName)
    conf.set(SftNameKey, name)
    conf.set(SftSpecKey, SimpleFeatureTypes.encodeType(sft, includeUserData = true))
  }
  def getSft(conf: Configuration): SimpleFeatureType =
    SimpleFeatureTypes.createType(conf.get(SftNameKey), conf.get(SftSpecKey))

  def getSftName(conf: Configuration): String = conf.get(SftNameKey)
  def getSftSpec(conf: Configuration): String = conf.get(SftSpecKey)

  def setRootPath(conf: Configuration, path: Path): Unit = conf.set(PathKey, path.toString)
  def getRootPath(conf: Configuration): Path = new Path(conf.get(PathKey))

  def setPartitions(conf: Configuration, partitions: Array[String]): Unit =
    conf.setStrings(PartitionsKey, partitions: _*)
  def getPartitions(conf: Configuration): Array[String] = conf.getStrings(PartitionsKey)

  def setFileType(conf: Configuration, fileType: FileType): Unit = conf.set(FileTypeKey, fileType.toString)
  def getFileType(conf: Configuration): FileType = FileType.withName(conf.get(FileTypeKey))

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

  def setPathActions(conf: Configuration, paths: Seq[StorageFilePath]): Unit = {
    paths.foreach { case StorageFilePath(f, path) =>
      conf.set(s"$PathActionKey.${path.getName}", s"${f.timestamp}:${f.action}")
    }
  }
  def getPathAction(conf: Configuration, path: Path): (Long, StorageFileAction) = {
    val Array(ts, action) = conf.get(s"$PathActionKey.${path.getName}").split(":")
    (ts.toLong, StorageFileAction.withName(action))
  }

  /**
    * Key used for merging feature updates.
    *
    * Implements hadoop writable for m/r, kryo serializable for spark, and comparable to sort in
    * reverse chronological order
    */
  class SimpleFeatureAction extends Writable with KryoSerializable with Comparable[SimpleFeatureAction] {

    private var _id: String = _
    private var _timestamp: Long = _
    private var _action: StorageFileAction = _

    def this(id: String, timestamp: Long, action: StorageFileAction) = {
      this()
      this._id = id
      this._timestamp = timestamp
      this._action = action
    }

    def id: String = _id
    def timestamp: Long = _timestamp
    def action: StorageFileAction = _action

    override def compareTo(o: SimpleFeatureAction): Int = {
      var res = _id.compareTo(o.id)
      if (res == 0) {
        res = _timestamp.compareTo(o.timestamp) * -1 // note: reverse chronological sort
        if (res == 0) {
          res = _action.compareTo(o.action)
        }
      }
      res
    }

    override def write(out: DataOutput): Unit = {
      out.writeUTF(_id)
      out.writeLong(_timestamp)
      out.writeUTF(_action.toString)
    }

    override def readFields(in: DataInput): Unit = {
      _id = in.readUTF()
      _timestamp = in.readLong()
      _action = StorageFileAction.withName(in.readUTF())
    }

    override def write(kryo: Kryo, output: Output): Unit = {
      output.writeString(_id)
      output.writeLong(_timestamp)
      output.writeString(_action.toString)
    }

    override def read(kryo: Kryo, input: Input): Unit = {
      _id = input.readString()
      _timestamp = input.readLong()
      _action = StorageFileAction.withName(input.readString())
    }
  }
}

trait StorageConfiguration {
  def configureOutput(sft: SimpleFeatureType, job: Job): Unit
}
