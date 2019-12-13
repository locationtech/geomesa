/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security

import java.nio.charset.StandardCharsets

import org.junit.runner.RunWith
import org.locationtech.geomesa.security.VisibilityEvaluator._
import org.parboiled.errors.ParsingException
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class VisibilityEvaluatorTest extends Specification {

  val user: Array[Byte]  = "user".getBytes(StandardCharsets.UTF_8)
  val admin: Array[Byte] = "admin".getBytes(StandardCharsets.UTF_8)
  val test: Array[Byte]  = "test".getBytes(StandardCharsets.UTF_8)

  "VisibilityEvaluator" should {

    "be able to parse empty visibilities" >> {
      VisibilityEvaluator.parse(null) mustEqual VisibilityNone
      VisibilityEvaluator.parse("") mustEqual VisibilityNone
    }

    "be able to parse simple visibilities" >> {
      VisibilityEvaluator.parse("user") mustEqual VisibilityValue(user)
      VisibilityEvaluator.parse("'user'") mustEqual VisibilityValue(user)
      VisibilityEvaluator.parse("\"user\"") mustEqual VisibilityValue(user)
    }

    "be able to parse simple Boolean visibilities" >> {
      VisibilityEvaluator.parse("user&admin") mustEqual VisibilityAnd(Seq(VisibilityValue(user), VisibilityValue(admin)))
      VisibilityEvaluator.parse("'user'&admin") mustEqual VisibilityAnd(Seq(VisibilityValue(user), VisibilityValue(admin)))
      VisibilityEvaluator.parse("user&\"admin\"") mustEqual VisibilityAnd(Seq(VisibilityValue(user), VisibilityValue(admin)))
    }

    "be able to parse chained Boolean visibilities" >> {
      VisibilityEvaluator.parse("user&admin&test") mustEqual
          VisibilityAnd(Seq(VisibilityValue(user), VisibilityValue(admin), VisibilityValue(test)))
      VisibilityEvaluator.parse("user|admin|test") mustEqual
          VisibilityOr(Seq(VisibilityValue(user), VisibilityValue(admin), VisibilityValue(test)))
      VisibilityEvaluator.parse("user&'admin'&\"test\"") mustEqual
          VisibilityAnd(Seq(VisibilityValue(user), VisibilityValue(admin), VisibilityValue(test)))
    }

    "be able to parse nested Boolean visibilities" >> {
      VisibilityEvaluator.parse("(user&admin)|test") mustEqual
          VisibilityOr(Seq(VisibilityAnd(Seq(VisibilityValue(user), VisibilityValue(admin))), VisibilityValue(test)))
      VisibilityEvaluator.parse("(user&'admin')|'test'") mustEqual
          VisibilityOr(Seq(VisibilityAnd(Seq(VisibilityValue(user), VisibilityValue(admin))), VisibilityValue(test)))
      VisibilityEvaluator.parse("user&(admin|test)") mustEqual
          VisibilityAnd(Seq(VisibilityValue(user), VisibilityOr(Seq(VisibilityValue(admin), VisibilityValue(test)))))
      VisibilityEvaluator.parse("\"user\"&(\"admin\"|test)") mustEqual
          VisibilityAnd(Seq(VisibilityValue(user), VisibilityOr(Seq(VisibilityValue(admin), VisibilityValue(test)))))
      VisibilityEvaluator.parse("user|admin&test") mustEqual
          VisibilityOr(Seq(VisibilityValue(user), VisibilityAnd(Seq(VisibilityValue(admin), VisibilityValue(test)))))
      VisibilityEvaluator.parse("user&admin|test") mustEqual
          VisibilityOr(Seq(VisibilityAnd(Seq(VisibilityValue(user), VisibilityValue(admin))), VisibilityValue(test)))
    }

    "be able to parse complex quoted strings" >> {
      val complexString = "foo bar'"
      val complex = complexString.getBytes(StandardCharsets.UTF_8)
      VisibilityEvaluator.parse(complexString) must throwA[ParsingException]
      VisibilityEvaluator.parse('"' + complexString + '"') mustEqual VisibilityValue(complex)
      VisibilityEvaluator.parse(s"""(user&"$complexString")|test""") mustEqual
          VisibilityOr(Seq(VisibilityAnd(Seq(VisibilityValue(user), VisibilityValue(complex))), VisibilityValue(test)))
    }

    "evaluate authorizations" >> {
      VisibilityEvaluator.parse(null).evaluate(Seq(user, admin, test)) must beTrue
      VisibilityEvaluator.parse(null).evaluate(Seq(user)) must beTrue
      VisibilityEvaluator.parse(null).evaluate(Seq(admin, test)) must beTrue
      VisibilityEvaluator.parse(null).evaluate(Seq()) must beTrue
      VisibilityEvaluator.parse("").evaluate(Seq(user, admin, test)) must beTrue
      VisibilityEvaluator.parse("").evaluate(Seq(user)) must beTrue
      VisibilityEvaluator.parse("").evaluate(Seq(admin, test)) must beTrue
      VisibilityEvaluator.parse("").evaluate(Seq()) must beTrue
      VisibilityEvaluator.parse("user").evaluate(Seq(user, admin, test)) must beTrue
      VisibilityEvaluator.parse("user").evaluate(Seq(user)) must beTrue
      VisibilityEvaluator.parse("user").evaluate(Seq(admin, test)) must beFalse
      VisibilityEvaluator.parse("user").evaluate(Seq()) must beFalse
      VisibilityEvaluator.parse("user&admin&test").evaluate(Seq(user, admin, test)) must beTrue
      VisibilityEvaluator.parse("user&admin&test").evaluate(Seq(user, admin)) must beFalse
      VisibilityEvaluator.parse("user&admin&test").evaluate(Seq(test)) must beFalse
      VisibilityEvaluator.parse("user&admin&test").evaluate(Seq()) must beFalse
      VisibilityEvaluator.parse("user|admin|test").evaluate(Seq(user, admin, test)) must beTrue
      VisibilityEvaluator.parse("user|admin|test").evaluate(Seq(user, admin)) must beTrue
      VisibilityEvaluator.parse("user|admin|test").evaluate(Seq(test)) must beTrue
      VisibilityEvaluator.parse("user|admin|test").evaluate(Seq()) must beFalse
      VisibilityEvaluator.parse("(user&admin)|test").evaluate(Seq(user, admin, test)) must beTrue
      VisibilityEvaluator.parse("(user&admin)|test").evaluate(Seq(test)) must beTrue
      VisibilityEvaluator.parse("(user&admin)|test").evaluate(Seq(user, admin)) must beTrue
      VisibilityEvaluator.parse("(user&admin)|test").evaluate(Seq(admin)) must beFalse
      VisibilityEvaluator.parse("(user&admin)|test").evaluate(Seq()) must beFalse
    }

    "parse z and 9" >> {
      VisibilityEvaluator.parse("zZ9") mustEqual VisibilityValue("zZ9".getBytes(StandardCharsets.UTF_8))
    }

    "throw a ParseException if the expression is invalid" >> {
      VisibilityEvaluator.parse(" ") must throwA[ParsingException]
      VisibilityEvaluator.parse("&") must throwA[ParsingException]
      VisibilityEvaluator.parse("|") must throwA[ParsingException]
      VisibilityEvaluator.parse("user&admin&") must throwA[ParsingException]
      VisibilityEvaluator.parse("user|admin|") must throwA[ParsingException]
      VisibilityEvaluator.parse("&user&admin") must throwA[ParsingException]
      VisibilityEvaluator.parse("|user|admin") must throwA[ParsingException]
      VisibilityEvaluator.parse("user&(admin") must throwA[ParsingException]
      VisibilityEvaluator.parse("user|(admin") must throwA[ParsingException]
      VisibilityEvaluator.parse("user&admin)") must throwA[ParsingException]
      VisibilityEvaluator.parse("user|admin)") must throwA[ParsingException]
      VisibilityEvaluator.parse("(user&admin") must throwA[ParsingException]
      VisibilityEvaluator.parse("(user|admin") must throwA[ParsingException]
    }

    "recreate the expression" >> {
      VisibilityEvaluator.parse(null).expression mustEqual ""
      VisibilityEvaluator.parse("").expression mustEqual ""
      VisibilityEvaluator.parse("user").expression mustEqual "user"
      VisibilityEvaluator.parse("user&admin&test").expression mustEqual "user&admin&test"
      VisibilityEvaluator.parse("user|admin|test").expression mustEqual "user|admin|test"
      VisibilityEvaluator.parse("(user&admin)|test").expression mustEqual "(user&admin)|test"
    }

    "apply and unapply" >> {
      VisibilityExpression("(user&admin)|test") mustEqual VisibilityEvaluator.parse("(user&admin)|test")
      VisibilityEvaluator.parse("(user&admin)|test") match {
        case VisibilityExpression(expression) => expression mustEqual "(user&admin)|test"
        case _ => ko("didn't unapply")
      }
    }
  }
}
