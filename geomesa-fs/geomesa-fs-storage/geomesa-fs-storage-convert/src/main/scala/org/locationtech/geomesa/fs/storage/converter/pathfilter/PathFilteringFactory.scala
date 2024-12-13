/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter.pathfilter

import org.locationtech.geomesa.fs.storage.api.NamedOptions

import java.util.ServiceLoader

trait PathFilteringFactory {
  def load(config: NamedOptions): Option[PathFiltering]
}

object PathFilteringFactory {

  import scala.collection.JavaConverters._

  private lazy val factories = ServiceLoader.load(classOf[PathFilteringFactory]).asScala.toSeq

  def load(config: NamedOptions): Option[PathFiltering] = factories.toStream.flatMap(_.load(config)).headOption
}
