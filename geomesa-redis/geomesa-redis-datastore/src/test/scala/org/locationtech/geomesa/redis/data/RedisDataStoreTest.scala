/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data

import java.nio.charset.StandardCharsets
import java.util.Collections

import org.geotools.data.{DataStoreFinder, Query}
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata
import org.locationtech.geomesa.index.utils.ExplainString
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.matcher.Matcher
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import redis.clients.jedis.{Jedis, JedisPool}

@RunWith(classOf[JUnitRunner])
class RedisDataStoreTest extends Specification with Mockito {

  import scala.collection.JavaConverters._

  val spec = s"name:String:index=true,dtg:Date,*geom:Point:srid=4326;geomesa.indices=z3:${Z3Index.version}:3:geom:dtg"

  val sft = SimpleFeatureTypes.createImmutableType("test", spec)

  def bytes(s: String): Array[Byte] = s.getBytes(StandardCharsets.UTF_8)
  def matcher(s: String): Matcher[Array[Byte]] = beEqualTo(bytes(s))

  "RedisDataStore" should {
    "print explain plans" in {
      val catalog = "gm-test"

      val jedis = mock[Jedis]
      val connection = mock[JedisPool]
      connection.getResource returns jedis

      jedis.hgetAll(matcher(catalog)) returns
          Collections.singletonMap(bytes("test~" + GeoMesaMetadata.AttributesKey), bytes(spec))
      jedis.hget(matcher(catalog), matcher("test~" + GeoMesaMetadata.AttributesKey)) returns bytes(spec)

      val params = Map[String, Any](
        RedisDataStoreParams.ConnectionPoolParam.key -> connection,
        RedisDataStoreParams.RedisCatalogParam.key -> catalog
      )
      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[RedisDataStore]
      ds must not(beNull)

      try {
        ds.getSchema(sft.getTypeName) mustEqual sft
        val explain = new ExplainString()
        ds.getQueryPlan(new Query(sft.getTypeName), explainer = explain) must not(throwAn[Exception])
        // TODO GEOMESA-3035 remove this check when we implement a better work-around
        explain.toString must contain("unserializable state=???")
      } finally {
        ds.dispose()
      }
    }
  }
}
