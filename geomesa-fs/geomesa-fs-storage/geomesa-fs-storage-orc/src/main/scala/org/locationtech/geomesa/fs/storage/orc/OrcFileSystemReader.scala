/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.storage.ql.io.sarg.SearchArgument
import org.apache.orc.{OrcFile, Reader}
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.fs.storage.orc.utils.{OrcAttributeReader, OrcSearchArguments}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer

class OrcFileSystemReader(sft: SimpleFeatureType,
                          config: Configuration,
                          filter: Option[Filter],
                          transform: Option[(String, SimpleFeatureType)]) extends FileSystemPathReader {

  private val (options, columns) = {
    val options = new Reader.Options(config)
    val readOptions = OrcFileSystemReader.readOptions(sft, filter, transform)
    val columns = readOptions.columns.map(_.map(sft.indexOf))
    columns.foreach { c => options.include(OrcFileSystemReader.include(sft, c)) }
    // note: push down will exclude whole record batches, but doesn't actually filter inside a batch
    readOptions.pushDown.foreach { case (sarg, cols) => options.searchArgument(sarg, cols) }
    (options, columns)
  }

  override def read(path: Path): CloseableIterator[SimpleFeature] = new PathReader(path)

  private class PathReader(file: Path) extends CloseableIterator[SimpleFeature] {
    private val feature = new ScalaSimpleFeature(sft, "")
    private val transformed = transform.map { case (tdefs, tsft) =>
      val sf = TransformSimpleFeature(sft, tsft, tdefs)
      sf.setFeature(feature)
      sf
    }
    private val result = transformed.getOrElse(feature)

    private val reader = OrcFile.createReader(file, OrcFile.readerOptions(config).useUTCTimestamp(true))
    private val rows = reader.rows(options)
    private val batch = reader.getSchema.createRowBatch()
    private val attributeReader = if (batch.cols.length > 0) { OrcAttributeReader(sft, batch, columns) } else { null }

    private var staged: Boolean = false
    private var i = 0

    override def hasNext: Boolean = {
      if (staged) { true } else {
        stageNext()
        staged
      }
    }

    /**
      * Note: same feature is mutated and returned for each call to .next
      *
      * @return
      */
    override def next(): SimpleFeature = {
      if (staged || hasNext) {
        staged = false
        result
      } else {
        Iterator.empty.next
      }
    }

    override def close(): Unit = rows.close()

    private def stageNext(): Unit = {
      var loop = true
      while (loop) {
        if (i < batch.size) {
          attributeReader.apply(feature, i)
          i += 1
          if (OrcFileSystemReader.this.filter.forall(_.evaluate(feature))) {
            staged = true
            loop = false
          }
        } else {
          i = 0
          loop = rows.nextBatch(batch)
        }
      }
    }
  }
}

object OrcFileSystemReader {

  /**
    * Create low-level reading options used by ORC
    *
    * @param sft simple feature type
    * @param filter ecql filter
    * @param transform transform
    * @return
    */
  def readOptions(sft: SimpleFeatureType,
                  filter: Option[Filter],
                  transform: Option[(String, SimpleFeatureType)]): OrcReadOptions = {
    val columns = transform.map { case (tdefs, _) =>
      import scala.collection.JavaConversions._
      val fromFilter = filter.map(FilterHelper.propertyNames(_, sft)).getOrElse(Seq.empty)
      val fromTransform = TransformProcess.toDefinition(tdefs).flatMap { definition =>
        FilterHelper.propertyNames(definition.expression, sft)
      }
      (fromFilter ++ fromTransform).toSet
    }
    // push down will exclude whole record batches, but doesn't actually filter inside a batch
    val pushDown = filter.flatMap { f =>
      OrcSearchArguments(sft, OrcFileSystemStorage.createTypeDescription(sft), f)
    }
    OrcReadOptions(columns, pushDown)
  }

  /**
    * Construct the array used to include columns corresponding to the simple feature transform.
    *
    * Included columns don't just align with the top-level fields - complex fields need to have additional
    * entries in the array for their children.
    *
    * @param sft simple feature type
    * @param columns attributes to read
    * @param fid read fid
    * @return
    */
  private def include(sft: SimpleFeatureType, columns: Set[Int], fid: Boolean = true): Array[Boolean] = {
    val buffer = ArrayBuffer[Boolean](true) // outer struct

    var i = 0
    while (i < sft.getAttributeCount) {
      val bindings = ObjectType.selectType(sft.getDescriptor(i))
      val count = bindings.head match {
        case ObjectType.GEOMETRY =>
          bindings(1) match {
            case ObjectType.POINT => 2 // x + y
            case ObjectType.LINESTRING | ObjectType.MULTIPOINT => 4 // list of x, list of y
            case ObjectType.POLYGON | ObjectType.MULTILINESTRING => 6 // list of list of x, list of list of y
            case ObjectType.MULTIPOLYGON => 8 // list of list of list of x, list of list of list of y
          }
        case ObjectType.LIST => 2
        case ObjectType.MAP => 3
        case _ => 1
      }
      val include = columns.contains(i)
      var j = 0
      while (j < count) {
        buffer += include
        j += 1
      }
      i += 1
    }

    buffer += fid

    buffer.toArray
  }

  case class OrcReadOptions(columns: Option[Set[String]], pushDown: Option[(SearchArgument, Array[String])])
}
