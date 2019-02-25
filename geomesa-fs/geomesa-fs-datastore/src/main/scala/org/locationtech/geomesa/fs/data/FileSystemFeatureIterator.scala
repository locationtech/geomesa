/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import java.io.Closeable

import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.opengis.feature.simple.SimpleFeature

/**
  * Iterator for querying file system storage
  *
  * Note: implements Closeable and not AutoCloseable so that DelegateFeatureIterator will close it properly
  *
  * @param storage storage impl
  * @param query query
  * @param threads threads
  */
class FileSystemFeatureIterator(storage: FileSystemStorage, query: Query, threads: Int)
    extends java.util.Iterator[SimpleFeature] with Closeable {

  private val iter = storage.getReader(query, threads = threads)

  override def hasNext: Boolean = iter.hasNext
  override def next(): SimpleFeature = iter.next()
  override def close(): Unit = iter.close()
}

