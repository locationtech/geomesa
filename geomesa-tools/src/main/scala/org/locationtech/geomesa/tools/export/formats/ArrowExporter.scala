/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import org.apache.arrow.vector.ipc.message.IpcOption
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.arrow.ArrowProperties
import org.locationtech.geomesa.arrow.io.{DictionaryBuildingWriter, FormatVersion, SimpleFeatureArrowFileWriter}
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.tools.`export`.formats.FeatureExporter.ExportStream
import org.locationtech.geomesa.tools.export.formats.ArrowExporter.{BatchDelegate, DictionaryDelegate, EncodedDelegate}
import org.locationtech.geomesa.tools.export.formats.FeatureExporter.ByteCounterExporter
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io._

class ArrowExporter(stream: ExportStream, hints: Hints) extends ByteCounterExporter(stream) {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  private lazy val sort = hints.getArrowSort
  private lazy val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid, hints.isArrowProxyFid)
  private lazy val ipc = hints.getArrowFormatVersion.getOrElse(FormatVersion.ArrowFormatVersion.get)
  private lazy val batchSize = hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt)
  private lazy val dictionaryFields = hints.getArrowDictionaryFields

  private var delegate: FeatureExporter = _

  override def start(sft: SimpleFeatureType): Unit = {
    delegate = if (sft == org.locationtech.geomesa.arrow.ArrowEncodedSft) {
      new EncodedDelegate(stream.os)
    } else if (dictionaryFields.isEmpty) {
      // note: features should be sorted already, even if arrow encoding wasn't performed
      new BatchDelegate(stream.os, encoding, FormatVersion.options(ipc), sort, batchSize, Map.empty)
    } else {
      if (sort.isDefined) {
        throw new NotImplementedError("Sorting and calculating dictionaries at the same time is not supported")
      }
      new DictionaryDelegate(stream.os, dictionaryFields, encoding, FormatVersion.options(ipc), batchSize)
    }
    delegate.start(sft)
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = delegate.export(features)

  override def close(): Unit = {
    CloseWithLogging(Option(delegate))
    stream.close()
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
    override def bytes: Long = 0L
    override def close(): Unit = {}
  }

  private class DictionaryDelegate(
      os: OutputStream,
      dictionaryFields: Seq[String],
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      batchSize: Int
    ) extends FeatureExporter {

    private var writer: DictionaryBuildingWriter = _
    private var count = 0L

    override def start(sft: SimpleFeatureType): Unit = {
      writer = new DictionaryBuildingWriter(sft, dictionaryFields, encoding, ipcOpts)
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

    override def bytes: Long = 0L

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
      dictionaries: Map[String, ArrowDictionary]
    ) extends FeatureExporter {

    private var writer: SimpleFeatureArrowFileWriter = _
    private var count = 0L

    override def start(sft: SimpleFeatureType): Unit = {
      writer = SimpleFeatureArrowFileWriter(os, sft, dictionaries, encoding, ipcOpts, sort)
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

    override def bytes: Long = 0L

    override def close(): Unit = {
      if (writer != null) {
        CloseWithLogging(writer)
      }
    }
  }
}
