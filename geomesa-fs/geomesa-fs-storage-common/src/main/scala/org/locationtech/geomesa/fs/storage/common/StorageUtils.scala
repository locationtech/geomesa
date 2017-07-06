/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import org.apache.hadoop.fs.{FileSystem, Path}
import org.locationtech.geomesa.fs.storage.api.PartitionScheme

object StorageUtils {

  def buildPartitionList(root: Path,
                         fs: FileSystem,
                         typeName: String,
                         partitionScheme: PartitionScheme,
                         fileExtension: String): List[String] = {

    def recurse(path: Path, prefix: String, curDepth: Int, maxDepth: Int): List[String] = {
      if (curDepth > maxDepth) return List.empty[String]
      val status = fs.listStatus(path)
      status.flatMap { f =>
        if (f.isDirectory) {
          recurse(f.getPath, s"$prefix${f.getPath.getName}/", curDepth + 1, maxDepth)
        } else if (f.getPath.getName.endsWith(s".$fileExtension")) {
          val name = f.getPath.getName.dropRight(fileExtension.length + 1)
          List(s"$prefix$name")
        } else List()
      }
    }.toList

    recurse(new Path(root, typeName), "", 0, partitionScheme.maxDepth())
  }

}
