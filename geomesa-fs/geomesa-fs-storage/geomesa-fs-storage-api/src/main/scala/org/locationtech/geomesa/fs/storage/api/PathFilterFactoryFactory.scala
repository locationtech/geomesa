/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api

import java.util.ServiceLoader

/**
  * Factory for loading path filters
  */
trait PathFilterFactoryFactory {

  /**
    * Load a path filter factory
    *
    * @param config factory config options
    * @return
    */
  def load(config: NamedOptions): Option[PathFilterFactory]
}

object PathFilterFactoryFactory {

  import scala.collection.JavaConverters._

  private lazy val factories = ServiceLoader.load(classOf[PathFilterFactoryFactory]).asScala.toSeq

  /**
    * Create a path filter factory instance via SPI lookup
    *
    * @param config factory config options
    * @return
    */
  def load(config: NamedOptions): Option[PathFilterFactory] = factories.toStream.flatMap(_.load(config)).headOption
}
