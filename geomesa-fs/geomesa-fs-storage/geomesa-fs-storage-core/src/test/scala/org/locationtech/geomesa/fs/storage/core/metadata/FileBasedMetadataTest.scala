/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.metadata

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import java.net.URI

@RunWith(classOf[JUnitRunner])
class FileBasedMetadataTest extends TestAbstractMetadata {

  override protected def metadataType: String = FileBasedMetadata.MetadataType
  override protected def getConfig(root: URI): Map[String, String] = Map.empty
}
