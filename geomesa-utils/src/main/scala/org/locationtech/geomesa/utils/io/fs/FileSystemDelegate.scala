/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io.fs

import java.io.InputStream

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

  trait FileHandle {
    def path: String
    def length: Long
    def open: InputStream
  }
}
