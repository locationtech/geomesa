/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka.confluent.ConfluentKafkaDataStoreFactoryTest.generateSchemaOverrideConfig
import org.locationtech.geomesa.kafka.confluent.ConfluentKafkaDataStoreTest._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConfluentKafkaDataStoreFactoryTest extends Specification {

  "parseSchemaOverrides" should {
    "fail to parse schemas" >> {
      "when the config cannot be parsed" in {
        val config = "{[[[{{{]]"
        val schemaJsonByTopic = Map(topic -> config)
        val schemaOverridesConfig = Some(generateSchemaOverrideConfig(schemaJsonByTopic))

        ConfluentKafkaDataStoreFactory.parseSchemaOverrides(schemaOverridesConfig) must
          throwAn[IllegalArgumentException]
      }

      "when the schema overrides key does not exist" in {
        val config =
          s"""{
             |  "key": "value"
             |}""".stripMargin
        val schemaOverridesConfig = Some(config)

        ConfluentKafkaDataStoreFactory.parseSchemaOverrides(schemaOverridesConfig) must
          throwAn[IllegalArgumentException]
      }

      "when one or more of the schemas cannot be parsed" in {
        val schemaJson =
          s"""{
             |  "type":"record",
             |  "name":"schema",
             |  "fields":[
             |    {
             |      "name":"shape",
             |      "type":"bad-type"
             |    }
             |  ]
             |}""".stripMargin
        val schemaJsonByTopic = Map(topic -> schemaJson)
        val schemaOverridesConfig = Some(generateSchemaOverrideConfig(schemaJsonByTopic))

        ConfluentKafkaDataStoreFactory.parseSchemaOverrides(schemaOverridesConfig) must
          throwAn[IllegalArgumentException]
      }

      "when one or more of the schemas cannot be converted to an SFT" in {
        val schemaJsonByTopic = Map(topic -> badSchemaJson)
        val schemaOverridesConfig = Some(generateSchemaOverrideConfig(schemaJsonByTopic))

        ConfluentKafkaDataStoreFactory.parseSchemaOverrides(schemaOverridesConfig) must
          throwAn[IllegalArgumentException]
      }
    }

    "parse no schemas" >> {
      "when there are no schema overrides defined" in {
        val schemaOverridesConfig = Some(generateSchemaOverrideConfig(Map.empty))

        ConfluentKafkaDataStoreFactory.parseSchemaOverrides(schemaOverridesConfig) mustEqual Map.empty
      }
    }

    "succeed in parsing schemas" >> {
      "when there is one valid schema" in {
        val schemaJsonByTopic = Map(topic -> schemaJson1)
        val schemaOverridesConfig = Some(generateSchemaOverrideConfig(schemaJsonByTopic))

        val results = ConfluentKafkaDataStoreFactory.parseSchemaOverrides(schemaOverridesConfig)

        results.size mustEqual 1

        val result = results(topic)
        SimpleFeatureTypes.encodeType(result._1, includeUserData = false) mustEqual encodedSft1
        result._2 mustEqual schema1
      }

      "when there are multiple valid schemas" in {
        val topic1 = "topic1"
        val topic2 = "topic2"

        val schemaJsonByTopic = Map(topic1 -> schemaJson1, topic2 -> schemaJson2)
        val schemaOverridesConfig = Some(generateSchemaOverrideConfig(schemaJsonByTopic))

        val results = ConfluentKafkaDataStoreFactory.parseSchemaOverrides(schemaOverridesConfig)

        results.size mustEqual 2

        val result1 = results(topic1)
        SimpleFeatureTypes.encodeType(result1._1, includeUserData = false) mustEqual encodedSft1
        result1._2 mustEqual schema1

        val result2 = results(topic2)
        SimpleFeatureTypes.encodeType(result2._1, includeUserData = false) mustEqual encodedSft2
        result2._2 mustEqual schema2
      }
    }
  }
}

object ConfluentKafkaDataStoreFactoryTest {

  def generateSchemaOverrideConfig(schemaJsonByTopic: Map[String, String]): String = {
    val schemas = schemaJsonByTopic.map { case (topic,json) => s""""$topic":$json""" }.mkString(",")
    s"""{"schemas":{$schemas}}"""
  }
}
