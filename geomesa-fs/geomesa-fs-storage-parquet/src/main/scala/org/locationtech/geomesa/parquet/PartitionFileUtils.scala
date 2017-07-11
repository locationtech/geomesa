/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}


class PartitionFileUtils(fs: FileSystem,
                         ext: String) extends LazyLogging {

  def nextFile(dir: Path): Path = {
    val existingFiles = getChildrenFile(dir).map(_.getName)
    var i = 0
    def nextName = f"$i%04d.$ext"
    var name = nextName
    while (existingFiles.contains(name)) {
      i += 1
      logger.info(s"Found file $name in directory $dir trying next file")
      name = nextName
    }
    new Path(dir, name)
  }

  def getChildrenFile(path: Path): Seq[Path] = {
    if (fs.exists(path)) {
      fs.listStatus(path).map { f => f.getPath }.filter(_.getName.endsWith(ext)).toSeq
    } else {
      Seq.empty[Path]
    }
  }

}
