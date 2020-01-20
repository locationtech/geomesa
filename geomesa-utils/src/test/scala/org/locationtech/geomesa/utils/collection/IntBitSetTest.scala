/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import com.esotericsoftware.kryo.io.{Input, Output}
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.collection.IntBitSet._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class IntBitSetTest extends Specification {

  val sizes = Seq(32, 64, 96, 128, 160, 192, 224) // gets each of the bit set impls

  val rands = new ThreadLocal[Random]() {
    override def initialValue: Random = new Random(-7)
  }

  def shuffle(count: Int): Seq[Int] = rands.get.shuffle((0 until count).toList)

  "IntBitSet" should {
    "set and get values" >> {
      foreach(sizes) { size =>
        val bs = IntBitSet(size)
        foreach(shuffle(size)) { i =>
          bs.contains(i) must beFalse
          bs.add(i) must beTrue
          bs.contains(i) must beTrue
          bs.add(i) must beFalse
        }
        foreach(shuffle(size)) { i =>
          bs.remove(i) must beTrue
          bs.contains(i) must beFalse
          bs.remove(i) must beFalse
          bs.contains(i) must beFalse
        }
      }
    }
    "clear values" >> {
      foreach(sizes) { size =>
        val bs = IntBitSet(size)
        foreach(shuffle(size)) { i =>
          bs.add(i) must beTrue
        }
        foreach(shuffle(size)) { i =>
          bs.contains(i) must beTrue
        }
        bs.clear()
        foreach(shuffle(size)) { i =>
          bs.contains(i) must beFalse
        }
      }
    }
    "have the correct size" >> {
      foreach(sizes) { size =>
        val bs = IntBitSet(size)
        size match {
          case 32  => bs must beAnInstanceOf[BitSet32]
          case 64  => bs must beAnInstanceOf[BitSet64]
          case 96  => bs must beAnInstanceOf[BitSet96]
          case 128 => bs must beAnInstanceOf[BitSet128]
          case 160 => bs must beAnInstanceOf[BitSet160]
          case 192 => bs must beAnInstanceOf[BitSet192]
          case 224 => bs must beAnInstanceOf[BitSetN]
        }
        bs.add(size - 1) must beTrue
        bs.contains(size - 1) must beTrue
        bs.remove(size - 1) must beTrue
        bs.contains(size - 1) must beFalse
      }
    }
    "be serializable" >> {
      val indices = Seq.tabulate(7)(i => rands.get.nextInt(32) + (32 * i))
      foreach(sizes) { size =>
        val bs = IntBitSet(size)
        indices.foreach { i =>
          if (i < size) {
            bs.add(i)
          }
        }
        val output = new Output(32)
        bs.serialize(output)
        val recreated = IntBitSet.deserialize(new Input(output.toBytes), size)
        foreach(0 until size) { i =>
          recreated.contains(i) mustEqual indices.contains(i)
        }
      }
    }
  }
}
