/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.utils

import java.util.UUID

import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType.FileType

object StorageUtils {

  /**
    * Gets the base directory for a partition
    *
    * @param root root path
    * @param partition partition name
    * @param leaf leaf storage
    * @return
    */
  def baseDirectory(root: Path, partition: String, leaf: Boolean): Path =
    if (leaf) { new Path(root, partition).getParent } else { new Path(root, partition) }


  /**
    * Get the path for a new data file
    *
    * @param root storage root path
    * @param partition partition to write to
    * @param leaf leaf storage or not
    * @param extension file extension
    * @param fileType file type
    * @return
    */
  def nextFile(
      root: Path,
      partition: String,
      leaf: Boolean,
      extension: String,
      fileType: FileType): Path = {

    val uuid = UUID.randomUUID().toString.replaceAllLiterally("-", "")
    val file = s"$fileType$uuid.$extension"
    val name = if (leaf) { s"${partition.split('/').last}_$file" } else { file }
    new Path(baseDirectory(root, partition, leaf), name)
  }

  object FileType extends Enumeration {
    type FileType = Value
    val Written  : Value = Value("W")
    val Compacted: Value = Value("C")
    val Imported : Value = Value("I")
  }
}
