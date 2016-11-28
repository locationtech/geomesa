/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core.Session
import org.locationtech.geomesa.index.utils.GeoMesaMetadata


import scala.collection.JavaConversions._

class CassandraBackedMetaData(session: Session, catalog: String)
    extends GeoMesaMetadata[String]{

  def ensureTableExists(): Unit = {
    session.execute(s"CREATE TABLE IF NOT EXISTS $catalog (typeName text, key text, value text, PRIMARY KEY (typeName, key))")
  }

  override def getFeatureTypes: Array[String] = {
    ensureTableExists()
    session.execute(s"SELECT typeName FROM $catalog").all().map(row => row.getString("typeName")).toArray.distinct
  }

  override def insert(typeName: String, key: String, value: String): Unit = {
    ensureTableExists()
    session.execute(s"INSERT INTO $catalog (typeName, key, value) VALUES (?, ?, ?)", typeName, key, value)
  }

  override def insert(typeName: String, kvPairs: Map[String, String]): Unit = {
    ensureTableExists()
    kvPairs.foreach {case (k, v) => insert(typeName, k, v)}
  }

  override def remove(typeName: String, key: String): Unit = {
    ensureTableExists()
    session.execute(s"DELETE FROM $catalog WHERE typeName = ? AND key = ?", typeName, key)
  }

  override def read(typeName: String, key: String, cache: Boolean): Option[String] = {
    ensureTableExists()
    val rows = session.execute(s"SELECT value FROM $catalog WHERE typeName = ? AND key = ?", typeName, key).all()
    if (rows.length < 1) {
      None
    } else {
      Option(rows.head.getString("value"))
    }
  }

  override def invalidateCache(typeName: String, key: String): Unit = ???

  override def delete(typeName: String): Unit = {
    ensureTableExists()
    session.execute(s"DELETE FROM $catalog WHERE typeName = ?", typeName)
  }
}
