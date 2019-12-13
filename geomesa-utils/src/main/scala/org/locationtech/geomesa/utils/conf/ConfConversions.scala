/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

import com.typesafe.config.Config

object ConfConversions {

  implicit class RichConfig(val base: Config) {
    def getStringOpt(path: String): Option[String] =
      if (base.hasPath(path)) Some(base.getString(path)) else None

    def getBooleanOpt(path: String): Option[Boolean] =
      if (base.hasPath(path)) Some(base.getBoolean(path)) else None

    def getIntOpt(path: String): Option[Int] =
      if (base.hasPath(path)) Some(base.getInt(path)) else None

    def getLongOpt(path: String): Option[Long] =
      if (base.hasPath(path)) Some(base.getLong(path)) else None

    def getDoubleOpt(path: String): Option[Double] =
      if (base.hasPath(path)) Some(base.getDouble(path)) else None

    def getConfigOpt(path: String): Option[Config] =
      if (base.hasPath(path)) Some(base.getConfig(path)) else None

    def getConfigListOpt(path: String): Option[java.util.List[_ <: Config]] =
      if (base.hasPath(path)) Some(base.getConfigList(path)) else None

    def getStringListOpt(path: String): Option[java.util.List[String]] =
      if (base.hasPath(path)) Some(base.getStringList(path)) else None
  }
}
