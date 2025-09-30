/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
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
