/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql

import org.apache.spark.sql.types.UDTRegistration

package object jts {
  /**
   * This must be called before any JTS types are used.
   */
  def registerTypes(): Unit = registration

  /** Trick to defer initialization until `registerUDTs` is called,
   * and ensure its only called once per ClassLoader.
   */
  private[jts] lazy val registration: Unit = JTSTypes.typeMap.foreach {
    case (l, r) => UDTRegistration.register(l.getCanonicalName, r.getCanonicalName)
  }
}
