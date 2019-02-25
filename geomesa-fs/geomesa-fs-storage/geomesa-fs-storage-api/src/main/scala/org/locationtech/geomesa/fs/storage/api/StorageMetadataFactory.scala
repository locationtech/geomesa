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
  * Factory for loading metadata implementations
  */
trait StorageMetadataFactory {

  /**
    * Well-known name identifying this metadata implementation
    *
    * @return
    */
  def name: String

  /**
    * Load an existing metadata instance at the given path
    *
    * @param context file context
    * @return
    */
  def load(context: FileSystemContext): Option[StorageMetadata]

  /**
    * Create a metadata instance using the provided options
    *
    * @param context file context
    * @param config metadata configuration
    * @param meta simple feature type, file encoding, partition scheme, etc
    * @return
    */
  def create(context: FileSystemContext, config: Map[String, String], meta: Metadata): StorageMetadata
}

object StorageMetadataFactory {

  import scala.collection.JavaConverters._

  lazy private val factories = ServiceLoader.load(classOf[StorageMetadataFactory]).asScala.toSeq

  /**
    * Load a metadata instance from the given file context
    *
    * @param context file context
    * @return
    */
  def load(context: FileSystemContext): Option[StorageMetadata] =
    factories.toStream.flatMap(_.load(context)).headOption

  /**
    * Create a metadata instance using the provided options
    *
    * @param context file context
    * @param config metadata configuration
    * @param metadata simple feature type, file encoding, partition scheme, etc
    * @return
    */
  def create(context: FileSystemContext, config: NamedOptions, metadata: Metadata): StorageMetadata = {
    val factory = factories.find(_.name.equalsIgnoreCase(config.name)).getOrElse {
      throw new IllegalArgumentException(
        s"No storage metadata factory implementation exists for name '${config.name}'. Available factories: " +
            factories.map(_.name).mkString(", "))
    }
    factory.create(context, config.options, metadata)
  }
}
