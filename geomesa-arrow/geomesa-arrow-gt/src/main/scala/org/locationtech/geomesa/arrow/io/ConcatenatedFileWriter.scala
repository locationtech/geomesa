/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType

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
    // ensure we return something
    if (files.hasNext) { files } else {
      val dictionaries = dictionaryFields.mapWithIndex { case (name, i) =>
        name -> ArrowDictionary.create(i, Array.empty[AnyRef])
      }
      val os = new ByteArrayOutputStream()
      WithClose(SimpleFeatureArrowFileWriter(os, sft, dictionaries.toMap, encoding, ipcOpts, sort)) { writer =>
        writer.flush() // ensure header and dictionaries are written, and write an empty batch
      }
      // files is empty but this will pass it through to be closed
      files ++ CloseableIterator.single(os.toByteArray)
    }
  }
}
