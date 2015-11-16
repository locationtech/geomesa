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

    def getConfigOpt(path: String) : Option[Config] =
      if (base.hasPath(path)) Some(base.getConfig(path)) else None
  }
}
