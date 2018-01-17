/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcConf
import org.locationtech.geomesa.fs.storage.common.FileSystemStorageFactory

class OrcFileSystemStorageFactory extends FileSystemStorageFactory[OrcFileSystemStorage] {

  override val encoding: String = OrcFileSystemStorage.OrcEncoding

  override protected def build(path: Path,
                               conf: Configuration,
                               params: java.util.Map[String, java.io.Serializable]): OrcFileSystemStorage = {
    if (conf.get(OrcConf.USE_ZEROCOPY.getAttribute) == null &&
          conf.get(OrcConf.USE_ZEROCOPY.getHiveConfName) == null) {
      OrcConf.USE_ZEROCOPY.setBoolean(conf, true)
    }
    new OrcFileSystemStorage(path.getFileSystem(conf), path, conf)
  }
}
