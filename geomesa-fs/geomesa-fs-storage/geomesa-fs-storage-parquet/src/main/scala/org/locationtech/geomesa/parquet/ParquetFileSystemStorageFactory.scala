/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage.ParquetCompressionOpt

class ParquetFileSystemStorageFactory extends FileSystemStorageFactory {

  override def encoding: String = ParquetFileSystemStorage.Encoding

  override def apply(context: FileSystemContext, metadata: StorageMetadata): ParquetFileSystemStorage = {
    if (context.conf.get(ParquetCompressionOpt) == null) {
      Option(System.getProperty(ParquetCompressionOpt)).foreach(context.conf.set(ParquetCompressionOpt, _))
    }
    context.conf.set("parquet.filter.dictionary.enabled", "true")
    new ParquetFileSystemStorage(context, metadata)
  }
}
