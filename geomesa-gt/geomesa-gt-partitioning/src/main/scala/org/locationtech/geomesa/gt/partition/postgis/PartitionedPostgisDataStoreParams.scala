/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.Parameter

import java.util.Collections
import scala.concurrent.duration.Duration

object PartitionedPostgisDataStoreParams {

  val DbType =
    new Param(
      "dbtype",
      classOf[String],
      "Type",
      true,
      "postgis-partitioned",
      Collections.singletonMap(Parameter.LEVEL, "program")
    )

  val PreparedStatements =
    new Param(
      "preparedStatements",
      classOf[java.lang.Boolean],
      "Use prepared statements",
      false,
      java.lang.Boolean.FALSE
    )

  val IdleInTransactionTimeout =
    new Param(
      "idle_in_transaction_session_timeout",
      classOf[Timeout],
      "Transaction idle timeout (e.g. '2 minutes'). " +
          "See https://www.postgresql.org/docs/15/runtime-config-client.html#GUC-IDLE-IN-TRANSACTION-SESSION-TIMEOUT",
      false
    ) with TimeoutParam

  // note: need a default string constructor so geotools can create it from the param
  class Timeout(repr: String) {
    private val duration = Duration(repr)
    require(duration.isFinite && duration.gt(Duration.Zero), s"Invalid duration: $repr")

    val millis: Long = duration.toMillis
    val seconds: Int = math.ceil(millis / 1000.0).toInt
  }

  trait TimeoutParam extends Param {
    override def lookUp(map: java.util.Map[String, _]): Timeout =
      super.lookUp(map) match {
        case t: Timeout => t
        case _ => null
      }

    def opt(map: java.util.Map[String, _]): Option[Timeout] = Option(lookUp(map))
  }
}
