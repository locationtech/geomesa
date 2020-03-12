/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.Connector

object AccumuloVersion extends LazyLogging {

  /**
   * Closes the connector. Note that in Accumulo 1.x, there is no way to close a connector. In 2.0,
   * the connector wraps an AccumuloClient that is closeable
   *
   * @param connector connector
   */
  def close(connector: Connector): Unit = {
    try {
      val impl = Class.forName("org.apache.accumulo.core.clientImpl.ConnectorImpl")
      val client = impl.getDeclaredMethod("getAccumuloClient").invoke(connector)
      client.asInstanceOf[AutoCloseable].close()
    } catch {
      case _: ClassNotFoundException => // accumulo 1.x
    }
  }
}
