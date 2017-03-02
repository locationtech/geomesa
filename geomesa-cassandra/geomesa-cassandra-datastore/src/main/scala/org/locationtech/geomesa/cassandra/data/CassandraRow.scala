/***********************************************************************
* Copyright (c) 2016  IBM
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core._

case class CassandraRow(start: Option[Array[Byte]] = None,
  end: Option[Array[Byte]] = None,
  stmt:Option[Statement] = None,
  qs:Option[String] = None,
  values:Option[Seq[Object]]=None)
