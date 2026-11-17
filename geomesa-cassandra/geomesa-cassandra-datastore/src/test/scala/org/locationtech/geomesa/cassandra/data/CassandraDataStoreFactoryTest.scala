/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import org.junit.runner.RunWith
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.Params
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CassandraDataStoreFactoryTest extends Specification {

  import scala.collection.JavaConverters._

  "CassandraDataStoreFactory" should {
    "reject dotted catalogs before processing connection parameters" in {
      val factory = new CassandraDataStoreFactory
      forall(Seq("abc.foobar", ".foobar", "foobar.", "a.b.c")) { catalog =>
        val params = Map(Params.CatalogParam.key -> catalog).asJava
        (factory.createDataStore(params) must throwAn[IllegalArgumentException]("Specify the keyspace separately")) and
            (factory.createNewDataStore(params) must throwAn[IllegalArgumentException]("Specify the keyspace separately"))
      }
    }

    "accept an unqualified catalog and continue processing connection parameters" in {
      val params = Map(
        Params.CatalogParam.key -> "foobar",
        Params.ContactPointParam.key -> "invalid"
      ).asJava
      new CassandraDataStoreFactory().createDataStore(params) must
          throwAn[IllegalArgumentException](s"Invalid parameter '${Params.ContactPointParam.key}'")
    }
  }
}
