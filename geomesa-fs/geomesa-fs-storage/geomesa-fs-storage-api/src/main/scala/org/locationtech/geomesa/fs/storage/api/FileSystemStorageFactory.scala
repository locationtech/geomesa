/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api

import java.util.ServiceLoader

/**
  * Factory for creating and loading file system storage implementations
  */
trait FileSystemStorageFactory {

  /**
    * The file encoding used by this factory
    *
    * @return encoding
    */
  def encoding: String

  /**
    * Create a new storage instance pointing at the given context path
    *
    * @param context file context
    * @param metadata metadata persistence
    * @return
    */
  def apply(context: FileSystemContext, metadata: StorageMetadata): FileSystemStorage
}

object FileSystemStorageFactory {

  import scala.collection.JavaConverters._

  lazy private val factories = ServiceLoader.load(classOf[FileSystemStorageFactory]).asScala.toSeq

  /**
    * Create a file system storage instance pointing at the given context path
    *
    * @param context file context
    * @param metadata metadata persistence
    * @return
    */
  def apply(context: FileSystemContext, metadata: StorageMetadata): FileSystemStorage = {
    val factory = factories.find(_.encoding.equalsIgnoreCase(metadata.encoding)).getOrElse {
      throw new IllegalArgumentException(s"Could not find a factory class for encoding '${metadata.encoding}'. " +
          s"Factories are available for: ${factories.map(_.encoding).mkString(", ")}")
    }
    factory.apply(context, metadata)
  }
}
