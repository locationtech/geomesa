/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow

import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.ipc.message.IpcOption
import org.apache.arrow.vector.types.MetadataVersion
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.arrow.io.records.RecordBatchLoader
import org.locationtech.geomesa.arrow.jts.{GeometryFields, GeometryVector}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.conf.SemanticVersion
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Geometry

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.util.Collections

package object io {

  object Metadata {
    val SortField = "sort-field"
    val SortOrder = "sort-order"
  }

  object FormatVersion {

    @deprecated("Replaced with DefaultVersion")
    val LatestVersion = "18.3.0"

    // default format version, currently the only thing we care about here is whether it is before or after 0.15
    val DefaultVersion = "1.0.0"

    val ArrowFormatVersion: SystemProperty = SystemProperty("geomesa.arrow.format.version", DefaultVersion)

    def options(version: String): IpcOption = {
      lazy val semver = SemanticVersion(version, lenient = true) // avoid parsing if it's a known version (0.10)
      val legacy = version.startsWith("0") && (version == "0.10" || (semver.major == 0 && semver.minor < 15))
      val meta = if (legacy) { MetadataVersion.V4 } else { MetadataVersion.DEFAULT }
      new IpcOption(legacy, meta)
    }

    def version(opt: IpcOption): String = if (opt.write_legacy_ipc_format) { "0.10" } else { DefaultVersion }
  }

  /**
   * Checks schema metadata for sort fields
   *
   * @param metadata schema metadata
   * @return (sort field, reverse sorted or not)
   */
  def getSortFromMetadata(metadata: java.util.Map[String, String]): Option[(String, Boolean)] = {
    Option(metadata.get(Metadata.SortField)).map { field =>
      val reverse = Option(metadata.get(Metadata.SortOrder)).exists {
        case "descending" => true
        case _ => false
      }
      (field, reverse)
    }
  }

  /**
   * Creates metadata for sort fields
   *
   * @param field sort field
   * @param reverse reverse sorted or not
   * @return metadata map
   */
  def getSortAsMetadata(field: String, reverse: Boolean): java.util.Map[String, String] = {
    import scala.collection.JavaConverters._
    // note: reverse == descending
    Map(Metadata.SortField -> field, Metadata.SortOrder -> (if (reverse) { "descending" } else { "ascending" })).asJava
  }

  /**
   * Creates a vector schema root for the given vector
   *
   * @param vector vector
   * @param metadata field metadata
   * @return
   */
  def createRoot(vector: FieldVector, metadata: java.util.Map[String, String] = null): VectorSchemaRoot = {
    val schema = new Schema(Collections.singletonList(vector.getField), metadata)
    new VectorSchemaRoot(schema, Collections.singletonList(vector), vector.getValueCount)
  }

  /**
   * Create a transfer pair between two vectors. This handles geometry vectors correctly, which the underlying
   * arrow transfer pairs do not.
   *
   * @param from from vector
   * @param to to vector
   * @return transfer(fromIndex, toIndex)
   */
  def createTransferPair(sft: SimpleFeatureType, from: FieldVector, to: FieldVector): (Int, Int) => Unit = {
    val i = sft.indexOf(from.getField.getName)
    lazy val binding = sft.getDescriptor(i).getType.getBinding
    if (i != -1 && classOf[Geometry].isAssignableFrom(binding)) {
      // geometry vectors use FixedSizeList vectors, for which transfer pairs aren't implemented
      val fromGeom = GeometryFields.wrap(from, binding).asInstanceOf[GeometryVector[Geometry, FieldVector]]
      val toGeom = GeometryFields.wrap(to, binding).asInstanceOf[GeometryVector[Geometry, FieldVector]]
      (fromIndex: Int, toIndex: Int) => fromGeom.transfer(fromIndex, toIndex, toGeom)
    } else {
      val transfer = from.makeTransferPair(to)
      (fromIndex: Int, toIndex: Int) => transfer.copyValueSafe(fromIndex, toIndex)
    }
  }

  /**
   * Write out the header, dictionaries, and first batch of an arrow streaming file
   *
   * @param result vector loaded with first batch
   * @param dictionaries dictionaries
   * @param sort sort
   * @param count number of records in first batch
   * @return
   */
  def writeHeaderAndFirstBatch(
      result: SimpleFeatureVector,
      dictionaries: Map[String, ArrowDictionary],
      ipcOpts: IpcOption,
      sort: Option[(String, Boolean)],
      count: Int): Array[Byte] = {
    val metadata = sort match {
      case None => null
      case Some((sortBy, reverse)) => getSortAsMetadata(sortBy, reverse)
    }
    // note: don't close root as it will close the underlying vector
    val root = createRoot(result.underlying, metadata)
    root.setRowCount(count)
    val os = new ByteArrayOutputStream()
    WithClose(SimpleFeatureArrowFileWriter.provider(dictionaries, result.encoding)) { provider =>
      WithClose(new ArrowStreamWriter(root, provider, Channels.newChannel(os), ipcOpts)) { writer =>
        writer.writeBatch()
        os.toByteArray
      }
    }
  }

  /**
   * Create an arrow file from record batches
   *
   * @param sft simple feature type
   * @param dictionaries dictionaries
   * @param encoding feature encoding
   * @param sort sorting of the batches, if any
   * @param batches batches
   * @param firstBatchHasHeader does the first batch have the arrow file header or not
   * @return
   */
  def createFileFromBatches(
      sft: SimpleFeatureType,
      dictionaries: Map[String, ArrowDictionary],
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      sort: Option[(String, Boolean)],
      batches: CloseableIterator[Array[Byte]],
      firstBatchHasHeader: Boolean): CloseableIterator[Array[Byte]] = {
    val ft = if (ipcOpts.write_legacy_ipc_format) { legacyFooter } else { footer }
    new ArrowFileIterator(sft, dictionaries, encoding, sort, ipcOpts, batches, firstBatchHasHeader, ft)
  }

  // per arrow streaming format footer is the encoded int -1, 0
  private def footer: Array[Byte] = Array[Byte](-1, -1, -1, -1, 0, 0, 0, 0)
  private def legacyFooter: Array[Byte] = Array[Byte](0, 0, 0, 0)

  private class ArrowFileIterator(
      sft: SimpleFeatureType,
      dictionaries: Map[String, ArrowDictionary],
      encoding: SimpleFeatureEncoding,
      sort: Option[(String, Boolean)],
      ipcOpts: IpcOption,
      batches: CloseableIterator[Array[Byte]],
      firstBatchHasHeader: Boolean,
      footer: Array[Byte]
    ) extends CloseableIterator[Array[Byte]] {

    private var seenBatch = false
    private var seenFooter = false

    override def hasNext: Boolean = batches.hasNext || !seenBatch || !seenFooter

    override def next(): Array[Byte] = {
      if (seenBatch) {
        if (batches.hasNext) {
          batches.next()
        } else if (seenFooter) {
          throw new NoSuchElementException("Next on an empty iterator")
        } else {
          seenFooter = true
          footer
        }
      } else {
        seenBatch = true
        if (batches.hasNext) {
          if (firstBatchHasHeader) { batches.next } else {
            // add the file header and dictionaries
            WithClose(SimpleFeatureVector.create(sft, dictionaries, encoding)) { vector =>
              new RecordBatchLoader(vector.underlying).load(batches.next)
              writeHeaderAndFirstBatch(vector, dictionaries, ipcOpts, sort, vector.reader.getValueCount)
            }
          }
        } else {
          // write out an empty batch so that we get the header and dictionaries
          WithClose(SimpleFeatureVector.create(sft, dictionaries, encoding, 0)) { vector =>
            writeHeaderAndFirstBatch(vector, dictionaries, ipcOpts, sort, 0)
          }
        }
      }
    }

    override def close(): Unit = batches.close()
  }
}
