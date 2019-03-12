/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io.fs

import java.io.InputStream

import org.apache.commons.compress.compressors.bzip2.BZip2Utils
import org.apache.commons.compress.compressors.gzip.GzipUtils
import org.apache.commons.compress.compressors.xz.XZUtils
import org.apache.commons.io.FilenameUtils
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle

trait FileSystemDelegate {

  /**
    * Expand wildcards, recurse into directories, etc
    *
    * @param path input path
    * @return any files found in the interpreted path
    */
  def interpretPath(path: String): Seq[FileHandle]
}

object FileSystemDelegate {

  /**
    * Abstraction over a readable file
    */
  trait FileHandle {

    /**
      * The file extension, minus any compression or zipping
      *
      * @return
      */
    lazy val format: String = {
      val name = if (GzipUtils.isCompressedFilename(path)) {
        GzipUtils.getUncompressedFilename(path)
      } else if (XZUtils.isCompressedFilename(path)) {
        XZUtils.getUncompressedFilename(path)
      } else if (BZip2Utils.isCompressedFilename(path)) {
        BZip2Utils.getUncompressedFilename(path)
      } else {
        path
      }
      FilenameUtils.getExtension(name)
    }

    /**
      * Path to the underlying file represented by this object
      *
      * @return
      */
    def path: String

    /**
      * File length (size), in bytes
      *
      * @return
      */
    def length: Long

    /**
      * Open an input stream to read the underlying file. Archive formats (tar, zip) will return multiple streams,
      * one per archive entry, along with the name of the entry. The iterator of input streams should only be
      * closed once all the input streams have been processed. The individual streams will be closed when the
      * overall iterator is closed, although they may be closed individually if desired
      *
      * @return
      */
    def open: CloseableIterator[(Option[String], InputStream)]
  }
}
