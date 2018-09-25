/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import org.apache.hadoop.conf.Configuration
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.locationtech.geomesa.fs.storage.common.{StorageMetadata, FileSystemStorageFactory}
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage.{ParquetCompressionOpt, ParquetEncoding}

class ParquetFileSystemStorageFactory extends FileSystemStorageFactory {

  override def getEncoding: String = ParquetEncoding

  override protected def load(conf: Configuration, metadata: StorageMetadata): FileSystemStorage = {
    if (conf.get(ParquetCompressionOpt) == null) {
      Option(System.getProperty(ParquetCompressionOpt)).foreach(conf.set(ParquetCompressionOpt, _))
    }
    conf.set("parquet.filter.dictionary.enabled", "true")

    new ParquetFileSystemStorage(conf, metadata)
  }
}
