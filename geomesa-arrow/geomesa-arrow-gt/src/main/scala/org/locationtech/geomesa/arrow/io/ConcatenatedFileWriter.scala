/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.ByteArrayOutputStream

import org.apache.arrow.vector.ipc.message.IpcOption
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseQuietly, WithClose}
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

object ConcatenatedFileWriter {

  import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableLike

  /**
   * Reduce function for concatenating separate arrow files
   *
   * @param sft simple feature type
   * @param dictionaryFields dictionary fields
   * @param encoding simple feature encoding
   * @param sort sort
   * @param files full logical arrow files encoded in arrow streaming format
   * @return
   */
  def reduce(
      sft: SimpleFeatureType,
      dictionaryFields: Seq[String],
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      sort: Option[(String, Boolean)],
      files: CloseableIterator[Array[Byte]]): CloseableIterator[Array[Byte]] = {
    // NB: This is not a WithClose situation.
    //  If there is an empty/non-exceptional iterator, we wish to use it.
    //  If there are any issues, we wish to close the iterator to free memory.
    try {
      if (files.hasNext) {
        files
      } else {
        files.close() // Closing the input just to be sure.
        generateEmptyResponse(sft, dictionaryFields, encoding, ipcOpts, sort)
      }
    } catch {
      case NonFatal(e) =>
        CloseQuietly(files).foreach(e.addSuppressed)
        throw e
    }
  }

  private def generateEmptyResponse(sft: SimpleFeatureType, dictionaryFields: Seq[String], encoding: SimpleFeatureEncoding, ipcOpts: IpcOption, sort: Option[(String, Boolean)]) = {
    val dictionaries = dictionaryFields.mapWithIndex { case (name, i) =>
      name -> ArrowDictionary.create(sft.getTypeName, i, Array.empty[AnyRef])
    }
    val os = new ByteArrayOutputStream()
    WithClose(SimpleFeatureArrowFileWriter(os, sft, dictionaries.toMap, encoding, ipcOpts, sort)) { writer =>
      writer.flush() // ensure header and dictionaries are written, and write an empty batch
    }
    dictionaries.foreach { case (_, dictionary) => dictionary.close() }
    CloseableIterator.single(os.toByteArray)
  }
}
