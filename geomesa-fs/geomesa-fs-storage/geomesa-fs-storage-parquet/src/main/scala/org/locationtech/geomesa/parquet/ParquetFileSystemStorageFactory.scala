/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import java.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.storage.common.FileSystemStorageFactory
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage.{ParquetCompressionOpt, ParquetEncoding}

class ParquetFileSystemStorageFactory extends FileSystemStorageFactory[ParquetFileSystemStorage] {

  override val encoding: String = ParquetEncoding

  override protected def build(path: Path,
                               conf: Configuration,
                               params: java.util.Map[String, io.Serializable]): ParquetFileSystemStorage = {
    if (params.containsKey(ParquetCompressionOpt)) {
      conf.set(ParquetCompressionOpt, params.get(ParquetCompressionOpt).asInstanceOf[String])
    } else if (System.getProperty(ParquetCompressionOpt) != null) {
      conf.set(ParquetCompressionOpt, System.getProperty(ParquetCompressionOpt))
    }
    conf.set("parquet.filter.dictionary.enabled", "true")

    new ParquetFileSystemStorage(path, path.getFileSystem(conf), conf, params)
  }
}
