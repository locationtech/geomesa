/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka10.consumer.offsets

import com.typesafe.config.ConfigFactory
import kafka.message.Message
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner

@RunWith(classOf[runner.JUnitRunner])
class RequestedOffsetTest extends Specification {

  "RequestedOffset" should {
    "be configurable via typesafe config" >> {
      "for earliest" >> {
        val conf = ConfigFactory.parseString("{ offset = \"earliest\" }")
        RequestedOffset(conf) must beSome(EarliestOffset)
      }
      "for latest" >> {
        val conf = ConfigFactory.parseString("{ offset = \"latest\" }")
        RequestedOffset(conf) must beSome(LatestOffset)
      }
      "for dates" >> {
        val conf = ConfigFactory.parseString("{ offset = \"date:999\" }")
        RequestedOffset(conf) must beSome(DateOffset(999))
      }
      "for predicates" >> {
        val conf = ConfigFactory.parseString(s"{ offset = ${classOf[TestPredicate].getName} }")
        RequestedOffset(conf) must beAnInstanceOf[Some[FindOffset]]
      }
      "for specific numbers" >> {
        val conf = ConfigFactory.parseString("{ offset = \"5\"}")
        RequestedOffset(conf) must beSome(SpecificOffset(5))
      }
      "for empty" >> {
        val conf = ConfigFactory.parseString("{}")
        RequestedOffset(conf) must beNone
      }
      "for others" >> {
        val conf = ConfigFactory.parseString("{ offset = \"bogus\" }")
        RequestedOffset(conf) must beNone
      }
    }
  }
}

class TestPredicate extends FindMessage {
  override val predicate = (m: Message) => 0
}