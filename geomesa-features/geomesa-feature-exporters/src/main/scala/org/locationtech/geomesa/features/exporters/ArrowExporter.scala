/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.exporters

import org.apache.arrow.vector.ipc.message.IpcOption
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.arrow.ArrowProperties
import org.locationtech.geomesa.arrow.io.{DictionaryBuildingWriter, FormatVersion, SimpleFeatureArrowFileWriter}
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.exporters.ArrowExporter.{BatchDelegate, DictionaryDelegate, EncodedDelegate}
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io._

class ArrowExporter(out: OutputStream, hints: Hints) extends FeatureExporter {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  private lazy val sort = hints.getArrowSort
  private lazy val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid, hints.isArrowProxyFid, hints.isFlipAxisOrder)
  private lazy val ipc = hints.getArrowFormatVersion.getOrElse(FormatVersion.ArrowFormatVersion.get)
  private lazy val batchSize = hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt)
  private lazy val dictionaryFields = hints.getArrowDictionaryFields
  private lazy val flattenFields = hints.isArrowFlatten

  private var delegate: FeatureExporter = _

  override def start(sft: SimpleFeatureType): Unit = {
    delegate = if (sft == org.locationtech.geomesa.arrow.ArrowEncodedSft) {
      new EncodedDelegate(out)
    } else if (dictionaryFields.isEmpty) {
      // note: features should be sorted already, even if arrow encoding wasn't performed
      new BatchDelegate(out, encoding, FormatVersion.options(ipc), sort, batchSize, Map.empty, flattenFields)
    } else {
      if (sort.isDefined) {
        throw new NotImplementedError("Sorting and calculating dictionaries at the same time is not supported")
      }
      new DictionaryDelegate(out, dictionaryFields, encoding, FormatVersion.options(ipc), batchSize, flattenFields)
    }
    delegate.start(sft)
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = delegate.export(features)

  override def close(): Unit = {
    CloseWithLogging(Option(delegate))
    out.close()
  }
}

object ArrowExporter {

  private class EncodedDelegate(os: OutputStream) extends FeatureExporter {
    override def start(sft: SimpleFeatureType): Unit = {}
    override def export(features: Iterator[SimpleFeature]): Option[Long] = {
      // just copy bytes directly out
      features.foreach(f => os.write(f.getAttribute(0).asInstanceOf[Array[Byte]]))
      None // we don't know the actual count
    }
    override def close(): Unit = {}
  }

  private class DictionaryDelegate(
      os: OutputStream,
      dictionaryFields: Seq[String],
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      batchSize: Int,
      flattenStruct: Boolean = false
    ) extends FeatureExporter {

    private var writer: DictionaryBuildingWriter = _
    private var count = 0L

    override def start(sft: SimpleFeatureType): Unit = {
      writer = new DictionaryBuildingWriter(sft, dictionaryFields, encoding, ipcOpts, flattenStruct = flattenStruct)
    }

    override def export(features: Iterator[SimpleFeature]): Option[Long] = {
      val start = count
      features.foreach { f =>
        writer.add(f)
        count += 1
        if (count % batchSize == 0) {
          writer.encode(os)
          writer.clear()
        }
      }
      Some(count - start)
    }

    override def close(): Unit = {
      if (writer != null) {
        if (count % batchSize != 0) {
          writer.encode(os)
          writer.clear()
        }
        CloseWithLogging(writer)
      }
    }
  }

  private class BatchDelegate(
      os: OutputStream,
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      sort: Option[(String, Boolean)],
      batchSize: Int,
      dictionaries: Map[String, ArrowDictionary],
      flattenStruct: Boolean = false
    ) extends FeatureExporter {

    private var writer: SimpleFeatureArrowFileWriter = _
    private var count = 0L

    override def start(sft: SimpleFeatureType): Unit = {
      writer = SimpleFeatureArrowFileWriter(os, sft, dictionaries, encoding, ipcOpts, sort, flattenStruct = flattenStruct)
    }

    override def export(features: Iterator[SimpleFeature]): Option[Long] = {
      val start = count
      features.foreach { f =>
        writer.add(f)
        count += 1
        if (count % batchSize == 0) {
          writer.flush()
        }
      }
      Some(count - start)
    }

    override def close(): Unit = {
      if (writer != null) {
        CloseWithLogging(writer)
      }
    }
  }
}
