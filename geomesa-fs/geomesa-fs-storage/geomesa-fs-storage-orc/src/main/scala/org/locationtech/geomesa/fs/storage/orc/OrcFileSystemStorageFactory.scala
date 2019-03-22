/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc

import org.apache.orc.OrcConf
import org.locationtech.geomesa.fs.storage.api._

class OrcFileSystemStorageFactory extends FileSystemStorageFactory {

  override def encoding: String = OrcFileSystemStorage.Encoding

  override def apply(context: FileSystemContext, metadata: StorageMetadata): FileSystemStorage = {
    if (context.conf.get(OrcConf.USE_ZEROCOPY.getAttribute) == null &&
        context.conf.get(OrcConf.USE_ZEROCOPY.getHiveConfName) == null) {
      OrcConf.USE_ZEROCOPY.setBoolean(context.conf, true)
    }
    new OrcFileSystemStorage(context, metadata)
  }
}
