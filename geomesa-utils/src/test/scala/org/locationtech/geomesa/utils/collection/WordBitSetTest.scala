/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.collection.WordBitSet._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class WordBitSetTest extends Specification {

  val sizes = Seq(32, 64, 96, 128, 160, 192, 224) // gets each of the bit set impls

  val rands = new ThreadLocal[Random]() {
    override def initialValue: Random = new Random(-7)
  }

  def shuffle(count: Int): Seq[Int] = rands.get.shuffle((0 until count).toList)

  "WordBitSet" should {
    "set and get values" >> {
      foreach(sizes) { size =>
        val bs = WordBitSet(size)
        foreach(shuffle(size)) { i =>
          bs.word(i).contains(i) must beFalse
          bs.word(i).add(i) must beTrue
          bs.word(i).contains(i) must beTrue
          bs.word(i).add(i) must beFalse
        }
        foreach(shuffle(size)) { i =>
          bs.word(i).remove(i) must beTrue
          bs.word(i).contains(i) must beFalse
          bs.word(i).remove(i) must beFalse
          bs.word(i).contains(i) must beFalse
        }
      }
    }
    "have the correct size" >> {
      foreach(sizes) { size =>
        val bs = WordBitSet(size)
        size match {
          case 32  => bs must beAnInstanceOf[WordBitSet32]
          case 64  => bs must beAnInstanceOf[WordBitSet64]
          case 96  => bs must beAnInstanceOf[WordBitSet96]
          case 128 => bs must beAnInstanceOf[WordBitSet128]
          case 160 => bs must beAnInstanceOf[WordBitSet160]
          case 192 => bs must beAnInstanceOf[WordBitSet192]
          case 224 => bs must beAnInstanceOf[WordBitSetN]
        }
        bs.word(size - 1).add(size - 1) must beTrue
        bs.word(size - 1).contains(size - 1) must beTrue
        bs.word(size - 1).remove(size - 1) must beTrue
        bs.word(size - 1).contains(size - 1) must beFalse
      }
    }
  }
}
