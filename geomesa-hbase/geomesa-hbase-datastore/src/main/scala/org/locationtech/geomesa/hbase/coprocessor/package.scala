/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase

import org.apache.hadoop.hbase.Coprocessor

package object coprocessor {
  lazy val coprocessorList: Seq[Class[_ <: Coprocessor]] = Seq(
    classOf[KryoLazyDensityCoprocessor]
  )
}
