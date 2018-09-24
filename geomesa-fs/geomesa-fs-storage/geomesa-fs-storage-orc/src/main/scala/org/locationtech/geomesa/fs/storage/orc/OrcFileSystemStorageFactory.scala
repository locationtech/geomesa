/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc

import org.apache.hadoop.conf.Configuration
import org.apache.orc.OrcConf
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.locationtech.geomesa.fs.storage.common.{StorageMetadata, FileSystemStorageFactory}

class OrcFileSystemStorageFactory extends FileSystemStorageFactory {

  override def getEncoding: String = OrcFileSystemStorage.OrcEncoding

  override protected def load(conf: Configuration, metadata: StorageMetadata): FileSystemStorage = {
    if (conf.get(OrcConf.USE_ZEROCOPY.getAttribute) == null &&
          conf.get(OrcConf.USE_ZEROCOPY.getHiveConfName) == null) {
      OrcConf.USE_ZEROCOPY.setBoolean(conf, true)
    }
    new OrcFileSystemStorage(conf, metadata)
  }
}
