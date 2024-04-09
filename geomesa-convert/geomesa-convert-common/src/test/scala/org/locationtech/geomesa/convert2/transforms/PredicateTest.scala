/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
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
        pred.apply(Array("", "1", "2")) must beFalse
        pred.apply(Array("", "1", "1")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
      }
    }
    "compare string not equals" >> {
      val pred = Predicate("$1 != $2")
      pred.apply(Array("", "1", "2")) must beTrue
      pred.apply(Array("", "1", "1")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
      pred.eval(Array("", "1", "2")) must beTrue
      pred.eval(Array("", "1", "1")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
      pred.eval(Array("", "1", "2")) must beTrue
      pred.eval(Array("", "1", "1")) must beFalse
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
      pred.eval(Array("", "1", "2")) must beTrue
      pred.eval(Array("", "1", "1")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
      pred.eval(Array("", "1", "2")) must beTrue
      pred.eval(Array("", "1", "1")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
      pred.eval(Array("", "1", "2")) must beTrue
      pred.eval(Array("", "1", "1")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
      pred.eval(Array("", "1", "2")) must beTrue
      pred.eval(Array("", "1", "1")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
      pred.eval(Array("", "1", "2")) must beTrue
      pred.eval(Array("", "1", "1")) must beFalse
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
    }
    "compare int equals" >> {
      foreach(Seq("intEq($1::int, $2::int)", "$1::int == $2::int")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "1", "2")) must beFalse
        pred.apply(Array("", "1", "1")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
      }
    }
    "compare integer equals" >> {
      foreach(Seq("integerEq($1::int, $2::int)", "$1::int == $2::int")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "1", "2")) must beFalse
        pred.apply(Array("", "1", "1")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
      }
    }
    "compare nested int equals" >> {
      foreach(Seq("intEq($1::int, strlen($2))", "$1::int == strlen($2)")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "3", "foo")) must beTrue
        pred.apply(Array("", "4", "foo")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "3", "foo")) must beTrue
        pred.eval(Array("", "4", "foo")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "3", "foo")) must beTrue
        pred.eval(Array("", "4", "foo")) must beFalse
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "3", "foo")) must beTrue
        pred.eval(Array("", "4", "foo")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "3", "foo")) must beTrue
        pred.eval(Array("", "4", "foo")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "3", "foo")) must beTrue
        pred.eval(Array("", "4", "foo")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "3", "foo")) must beTrue
        pred.eval(Array("", "4", "foo")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "3", "foo")) must beTrue
        pred.eval(Array("", "4", "foo")) must beFalse
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
      }
    }
    "compare int lteq" >> {
      foreach(Seq("intLTEq($1::int, $2::int)", "$1::int <= $2::int")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "1", "2")) must beTrue
        pred.apply(Array("", "1", "1")) must beTrue
        pred.apply(Array("", "1", "0")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "1", "2")) must beTrue
        pred.eval(Array("", "1", "1")) must beTrue
        pred.eval(Array("", "1", "0")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
        pred.eval(Array("", "1", "2")) must beTrue
        pred.eval(Array("", "1", "1")) must beTrue
        pred.eval(Array("", "1", "0")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "1", "2")) must beTrue
        pred.eval(Array("", "1", "1")) must beTrue
        pred.eval(Array("", "1", "0")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
      }
    }
    "compare int lt" >> {
      foreach(Seq("intLT($1::int, $2::int)", "$1::int < $2::int")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "1", "2")) must beTrue
        pred.apply(Array("", "1", "1")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "1", "2")) must beTrue
        pred.eval(Array("", "1", "1")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "1", "2")) must beTrue
        pred.eval(Array("", "1", "1")) must beFalse
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "1", "2")) must beTrue
        pred.eval(Array("", "1", "1")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "1", "2")) must beTrue
        pred.eval(Array("", "1", "1")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "2")) must beTrue
        pred.eval(Array("", "1", "1")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "2")) must beTrue
        pred.eval(Array("", "1", "1")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "2")) must beTrue
        pred.eval(Array("", "1", "1")) must beFalse
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
      }
    }
    "compare int gteq" >> {
      foreach(Seq("intGTEq($1::int, $2::int)", "$1::int >= $2::int")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "1", "2")) must beFalse
        pred.apply(Array("", "1", "1")) must beTrue
        pred.apply(Array("", "2", "1")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
        pred.eval(Array("", "2", "1")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
        pred.eval(Array("", "2", "1")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beTrue
        pred.eval(Array("", "2", "1")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
      }
    }
    "compare int gt" >> {
      foreach(Seq("intGT($1::int, $2::int)", "$1::int > $2::int")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "1", "2")) must beFalse
        pred.apply(Array("", "1", "1")) must beFalse
        pred.apply(Array("", "2", "1")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beFalse
        pred.eval(Array("", "2", "1")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beFalse
        pred.eval(Array("", "2", "1")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "1", "2")) must beFalse
        pred.eval(Array("", "1", "1")) must beFalse
        pred.eval(Array("", "2", "1")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
      }
    }
    "compare double equals" >> {
      foreach(Seq("doubleEq($1::double, $2::double)", "$1::double == $2::double")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "1.0", "2.0")) must beFalse
        pred.apply(Array("", "1.0", "1.0")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
      }
    }
    "compare double lteq" >> {
      foreach(Seq("doubleLTEq($1::double, $2::double)", "$1::double <= $2::double")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "1.0", "2.0")) must beTrue
        pred.apply(Array("", "1.0", "1.0")) must beTrue
        pred.apply(Array("", "1.0", "0.0")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "1.0", "2.0")) must beTrue
        pred.eval(Array("", "1.0", "1.0")) must beTrue
        pred.eval(Array("", "1.0", "0.0")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
        pred.eval(Array("", "1.0", "2.0")) must beTrue
        pred.eval(Array("", "1.0", "1.0")) must beTrue
        pred.eval(Array("", "1.0", "0.0")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "1.0", "2.0")) must beTrue
        pred.eval(Array("", "1.0", "1.0")) must beTrue
        pred.eval(Array("", "1.0", "0.0")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
      }
    }
    "compare double lt" >> {
      foreach(Seq("doubleLT($1::double, $2::double)", "$1::double < $2::double")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "1.0", "2.0")) must beTrue
        pred.apply(Array("", "1.0", "1.0")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "1.0", "2.0")) must beTrue
        pred.eval(Array("", "1.0", "1.0")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "1.0", "2.0")) must beTrue
        pred.eval(Array("", "1.0", "1.0")) must beFalse
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "1.0", "2.0")) must beTrue
        pred.eval(Array("", "1.0", "1.0")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "1.0", "2.0")) must beTrue
        pred.eval(Array("", "1.0", "1.0")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1.0", "2.0")) must beTrue
        pred.eval(Array("", "1.0", "1.0")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1.0", "2.0")) must beTrue
        pred.eval(Array("", "1.0", "1.0")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1.0", "2.0")) must beTrue
        pred.eval(Array("", "1.0", "1.0")) must beFalse
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
      }
    }
    "compare double gteq" >> {
      foreach(Seq("doubleGTEq($1::double, $2::double)", "$1::double >= $2::double")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "1.0", "2.0")) must beFalse
        pred.apply(Array("", "1.0", "1.0")) must beTrue
        pred.apply(Array("", "2.0", "1.0")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beTrue
        pred.eval(Array("", "2.0", "1.0")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beTrue
        pred.eval(Array("", "2.0", "1.0")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beTrue
        pred.eval(Array("", "2.0", "1.0")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
      }
    }
    "compare double gt" >> {
      foreach(Seq("doubleGT($1::double, $2::double)", "$1::double > $2::double")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "1.0", "2.0")) must beFalse
        pred.apply(Array("", "1.0", "1.0")) must beFalse
        pred.apply(Array("", "2.0", "1.0")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beFalse
        pred.eval(Array("", "2.0", "1.0")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beFalse
        pred.eval(Array("", "2.0", "1.0")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "1.0", "2.0")) must beFalse
        pred.eval(Array("", "1.0", "1.0")) must beFalse
        pred.eval(Array("", "2.0", "1.0")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
      }
    }
    "compare not predicates" >> {
      foreach(Seq("not(strEq($1, $2))", "!($1 == $2)")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "1", "1")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        pred.eval(Array("", "1", "1")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "1", "1")) must beFalse
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "1", "1")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "1", "1")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "1")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "1")) must beFalse
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "1", "1")) must beFalse
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
        pred.eval(Array("", "1", "1")) must beFalse
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
=======
        pred.eval(Array("", "1", "1")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "1", "1")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
      }
    }
    "compare and predicates" >> {
      foreach(Seq("and(strEq($1, $2), strEq(concat($3, $4), $1))", "$1 == $2 && concat($3, $4) == $1")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "foo", "foo", "f", "oo")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
      }
    }
    "compare or predicates" >> {
      foreach(Seq("or(strEq($1, $2), strEq($3, $1))", "$1 == $2 || $3 == $1")) { s =>
        val pred = Predicate(s)
        pred.apply(Array("", "foo", "foo", "f", "oo")) must beTrue
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beTrue
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
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
        pred.apply(Array("", "foo", "foo", "f", "foo")) must beTrue
        pred.apply(Array("", "foo", "foo", "f", "oo")) must beFalse
        pred.apply(Array("", "foo", "fo", "f", "foo")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        pred.eval(Array("", "foo", "foo", "f", "foo")) must beTrue
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beFalse
        pred.eval(Array("", "foo", "fo", "f", "foo")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
        pred.eval(Array("", "foo", "foo", "f", "foo")) must beTrue
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beFalse
        pred.eval(Array("", "foo", "fo", "f", "foo")) must beFalse
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        pred.eval(Array("", "foo", "foo", "f", "foo")) must beTrue
        pred.eval(Array("", "foo", "foo", "f", "oo")) must beFalse
        pred.eval(Array("", "foo", "fo", "f", "foo")) must beFalse
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> locatelli-main
      }
    }
  }
}
