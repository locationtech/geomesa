/***********************************************************************
* Copyright (c) 2013-2016 IBM
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.index

import com.datastax.driver.core._
import org.locationtech.geomesa.cassandra.data._
import org.locationtech.geomesa.index.index.XZ3Index

case object CassandraXZ3Index
    extends CassandraFeatureIndex with XZ3Index[CassandraDataStore, CassandraFeature, Statement, Statement] {
  override val version: Int = 1
}
