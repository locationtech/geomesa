/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.EvaluationContext
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PredicateTest extends Specification {

  implicit val ctx: EvaluationContext = EvaluationContext.empty

  "Predicates" should {
    "compare string equals" >> {
      foreach(Seq("strEq($1, $2)", "$1 == $2")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
      }
    }
    "compare string not equals" >> {
      val pred = Predicate("$1 != $2")
      pred.eval(Array("", "1", "2")) must beTrue
      pred.eval(Array("", "1", "1")) must beFalse
    }
    "compare int equals" >> {
      foreach(Seq("intEq($1::int, $2::int)", "$1::int == $2::int")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
      }
    }
    "compare integer equals" >> {
      foreach(Seq("integerEq($1::int, $2::int)", "$1::int == $2::int")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
      }
    }
    "compare nested int equals" >> {
      foreach(Seq("intEq($1::int, strlen($2))", "$1::int == strlen($2)")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "3", "foo")) must beTrue
        pred.eval(Array("", "4", "foo")) must beFalse
      }
    }
    "compare int lteq" >> {
      foreach(Seq("intLTEq($1::int, $2::int)", "$1::int <= $2::int")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "1", "2")) must beTrue
        pred.eval(Array("", "1", "1")) must beTrue
        pred.eval(Array("", "1", "0")) must beFalse
      }
    }
    "compare int lt" >> {
      foreach(Seq("intLT($1::int, $2::int)", "$1::int < $2::int")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "1", "2")) must beTrue
        pred.eval(Array("", "1", "1")) must beFalse
      }
    }
    "compare int gteq" >> {
      foreach(Seq("intGTEq($1::int, $2::int)", "$1::int >= $2::int")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
        pred.eval(Array("", "2", "1")) must beTrue
      }
    }
    "compare int gt" >> {
      foreach(Seq("intGT($1::int, $2::int)", "$1::int > $2::int")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beFalse
        pred.eval(Array("", "2", "1")) must beTrue
      }
    }
    "compare double equals" >> {
      foreach(Seq("doubleEq($1::double, $2::double)", "$1::double == $2::double")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beTrue
      }
    }
    "compare double lteq" >> {
      foreach(Seq("doubleLTEq($1::double, $2::double)", "$1::double <= $2::double")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "1.0", "2.0")) must beTrue
        pred.eval(Array("", "1.0", "1.0")) must beTrue
        pred.eval(Array("", "1.0", "0.0")) must beFalse
      }
    }
    "compare double lt" >> {
      foreach(Seq("doubleLT($1::double, $2::double)", "$1::double < $2::double")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "1.0", "2.0")) must beTrue
        pred.eval(Array("", "1.0", "1.0")) must beFalse
      }
    }
    "compare double gteq" >> {
      foreach(Seq("doubleGTEq($1::double, $2::double)", "$1::double >= $2::double")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beTrue
        pred.eval(Array("", "2.0", "1.0")) must beTrue
      }
    }
    "compare double gt" >> {
      foreach(Seq("doubleGT($1::double, $2::double)", "$1::double > $2::double")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beFalse
        pred.eval(Array("", "2.0", "1.0")) must beTrue
      }
    }
    "compare not predicates" >> {
      foreach(Seq("not(strEq($1, $2))", "!($1 == $2)")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "1", "1")) must beFalse
      }
    }
    "compare and predicates" >> {
      foreach(Seq("and(strEq($1, $2), strEq(concat($3, $4), $1))", "$1 == $2 && concat($3, $4) == $1")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
      }
    }
    "compare or predicates" >> {
      foreach(Seq("or(strEq($1, $2), strEq($3, $1))", "$1 == $2 || $3 == $1")) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
      }
    }
    "compare grouped predicates" >> {
      val preds = Seq(
        "and(strEq($1, $4), or(strEq($1, $2), strEq($3, $1)))",
        "$1 == $4 && ($1 == $2 || $3 == $1)",
        "($1 == $2 || $3 == $1) && $1 == $4"
      )
      foreach(preds) { s =>
        val pred = Predicate(s)
        pred.eval(Array("", "foo", "foo", "f", "foo")) must beTrue
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beFalse
        pred.eval(Array("", "foo", "fo", "f", "foo")) must beFalse
      }
    }
  }
}
