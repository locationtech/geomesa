/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.storage.api._

class RawDirectoryMetadataFactory extends StorageMetadataFactory {

  override def name: String = RawDirectoryMetadata.MetadataType

  /**
    * Loads a metadata instance from an existing root. The metadata info is persisted in a `metadata.json`
    * file under the root path.
    *
    * Will return a cached instance, if available. If a previous check was made to load a file from this root,
    * and the file did not exist, will not re-attempt to load it until after a configurable timeout
    *
    * @see `org.locationtech.geomesa.fs.storage.common.utils.PathCache#CacheDurationProperty()`
    * @param context file context
    * @return
    **/
  override def load(context: FileSystemContext): Option[FileBasedMetadata] = {
    None //new RawDirectoryMetadata(context)
  }

  override def create(context: FileSystemContext, config: Map[String, String], meta: Metadata): RawDirectoryMetadata = {
//    val Metadata(_, encoding, _, leaf) = meta
//    val sft = namespaced(meta.sft, context.namespace)
//    // load the partition scheme first in case it fails
//    val scheme = PartitionSchemeFactory.load(sft, meta.scheme)
//    MetadataJson.writeMetadata(context, NamedOptions(name, config))
//    FileBasedMetadataFactory.write(context.fc, context.root, meta)
//    val directory = new Path(context.root, FileBasedMetadataFactory.MetadataDirectory)
//    val metadata = new FileBasedMetadata(context.fc, directory, sft, encoding, scheme, leaf)
//    FileBasedMetadataFactory.cache.put(FileBasedMetadataFactory.key(context), metadata)
//    metadata
    new RawDirectoryMetadata(context.fc, null, null, null, null, false)
  }
}


