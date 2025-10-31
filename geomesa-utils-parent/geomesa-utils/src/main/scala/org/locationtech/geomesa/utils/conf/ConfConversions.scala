/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

import com.typesafe.config.{Config, ConfigUtil}

object ConfConversions {

  import scala.collection.JavaConverters._

  /**
   * Helper methods on typesafe config objects
   *
   * @param base config
   */
  implicit class RichConfig(val base: Config) extends AnyVal {

    def getConfigOpt(path: String): Option[Config] =
      if (base.hasPath(path)) Some(base.getConfig(path)) else None

    def getConfigListOpt(path: String): Option[java.util.List[_ <: Config]] =
      if (base.hasPath(path)) Some(base.getConfigList(path)) else None

    /**
     * Converts the (potentially nested) config to a flat map
     *
     * @param delimiter delimiter used to join list values
     * @return
     */
    def toStringMap(delimiter: String = ","): Map[String, String] = {
      val entries = base.entrySet().asScala.map { e =>
        val value = e.getValue.unwrapped() match {
          case v: java.util.List[String] => String.join(delimiter, v)
          case v => s"$v"
        }
        String.join(".", ConfigUtil.splitPath(e.getKey)) -> value
      }
      entries.toMap
    }
  }
}
