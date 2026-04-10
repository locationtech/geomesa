/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
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

  lazy val factories: Seq[FileSystemStorageFactory] = ServiceLoader.load(classOf[FileSystemStorageFactory]).asScala.toSeq

  /**
   * Load a factory with the given encoding
   *
   * @param encoding file encoding
   * @return
   */
  def apply(encoding: String): FileSystemStorageFactory =
    factories.find(_.encoding.equalsIgnoreCase(encoding)).getOrElse {
      throw new IllegalArgumentException(s"Could not find a factory class for encoding '$encoding'. " +
        s"Factories are available for: ${factories.map(_.encoding).mkString(", ")}")
    }
}
