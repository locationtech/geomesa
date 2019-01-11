/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

/**
  * Adapted from:
  *
  * stream-lib
  * Copyright (C) 2011 Clearspring Technologies, Inc.
  * Copyright 2016 AddThis
  *
  * This product includes software developed by AddThis.
  *
  * This product also includes code adapted from:
  *
  * Apache Solr (http://lucene.apache.org/solr/)
  * Copyright 2014 The Apache Software Foundation
  *
  * Apache Mahout (http://mahout.apache.org/)
  * Copyright 2014 The Apache Software Foundation
  *
  */

package org.locationtech.geomesa.utils.clearspring

import com.clearspring.analytics.hash.MurmurHash
import com.clearspring.analytics.stream.cardinality.ICardinality

/**
  * Scala implementation of HyperLogLog (HLL) algorithm from this paper:
  * <p/>
  * http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
  * <p/>
  * HLL is an improved version of LogLog that is capable of estimating
  * the cardinality of a set with accuracy = 1.04/sqrt(m) where
  * m = math.pow(2, b).  So we can control accuracy vs space usage by increasing
  * or decreasing b.
  * <p/>
  * The main benefit of using HLL over LL is that it only requires 64%
  * of the space that LL does to get the same accuracy.
  * <p/>
  * This implementation implements a single counter.  If a large (millions)
  * number of counters are required you may want to refer to:
  * <p/>
  * http://dsiutils.dsi.unimi.it/
  * <p/>
  * It has a more complex implementation of HLL that supports multiple counters
  * in a single object, drastically reducing the java overhead from creating
  * a large number of objects.
  * <p/>
  * This implementation leveraged a javascript implementation that Yammer has
  * been working on:
  * <p/>
  * https://github.com/yammer/probablyjs
  * <p>
  * Note that this implementation does not include the long range correction function
  * defined in the original paper.  Empirical evidence shows that the correction
  * function causes more harm than good.
  * </p>
  * <p/>
  * <p>
  * Users have different motivations to use different types of hashing functions.
  * Rather than try to keep up with all available hash functions and to remove
  * the concern of causing future binary incompatibilities this class allows clients
  * to offer the value in hashed int or long form.  This way clients are free
  * to change their hash function on their own time line.  We recommend using Google's
  * Guava Murmur3_128 implementation as it provides good performance and speed when
  * high precision is required.  In our tests the 32bit MurmurHash function included
  * in this project is faster and produces better results than the 32 bit murmur3
  * implementation google provides.
  * </p>
  */
/**
  * Create a new HyperLogLog instance.  The log2m parameter defines the accuracy of
  * the counter.  The larger the log2m the better the accuracy.
  * <p/>
  * accuracy = 1.04/sqrt(math.pow(2, log2m))
  *
  * @param log2m - the number of bits to use as the basis for the HLL instance
  */
class HyperLogLog private (val log2m: Int, val registerSet: RegisterSet) extends ICardinality {

  require(log2m >= 0 && log2m <= 30, s"log2m argument $log2m is outside the acceptable range [0, 30]")

  private val alphaMM: Double = HyperLogLog.getAlphaMM(log2m, 1 << log2m)

  override def offerHashed(hashedValue: Long): Boolean = {
    // j becomes the binary address determined by the first b log2m of x
    // j will be between 0 and 2^log2m
    val j = (hashedValue >>> (java.lang.Long.SIZE - log2m)).asInstanceOf[Int]
    val r = java.lang.Long.numberOfLeadingZeros((hashedValue << log2m) | (1 << (log2m - 1)) + 1) + 1
    registerSet.updateIfGreater(j, r)
  }

  override def offerHashed(hashedValue: Int): Boolean = {
    // j becomes the binary address determined by the first b log2m of x
    // j will be between 0 and 2^log2m
    val j = hashedValue >>> (Integer.SIZE - log2m)
    val r = Integer.numberOfLeadingZeros((hashedValue << log2m) | (1 << (log2m - 1)) + 1) + 1
    registerSet.updateIfGreater(j, r)
  }

  override def offer(o: Any): Boolean = offerHashed(MurmurHash.hash(o))

  override def cardinality: Long = {
    var registerSum = 0d
    var zeros = 0.0

    var j = 0
    while (j < registerSet.count) {
      val value = registerSet.get(j)
      registerSum += 1.0 / (1 << value)
      if (value == 0) {
        zeros += 1
      }
      j += 1
    }

    val estimate = alphaMM * (1 / registerSum)

    if (estimate <= (5.0 / 2.0) * registerSet.count) {
      // Small Range Estimate
      math.round(HyperLogLog.linearCounting(registerSet.count, zeros))
    } else {
      math.round(estimate)
    }
  }

  override def sizeof(): Int = registerSet.size * 4

  /**
    * Add all the elements of the other set to this set.
    * <p/>
    * This operation does not imply a loss of precision.
    *
    * @param other A compatible Hyperloglog instance (same log2m)
    * @throws IllegalArgumentException if other is not compatible
    */
  def +=(other: HyperLogLog): Unit = {
    require(registerSet.size == other.registerSet.size, "Cannot merge estimators of different sizes")
    registerSet.merge(other.registerSet)
  }

  override def merge(estimators: ICardinality*): HyperLogLog = {
    val merged = new HyperLogLog(log2m, new RegisterSet(this.registerSet.count)())
    merged += this
    estimators.foreach {
      case h: HyperLogLog => merged += h
      case e => throw new IllegalArgumentException(s"Cannot merge estimators of different class: $e")
    }
    merged
  }

  override def getBytes: Array[Byte] = throw new NotImplementedError
}

object HyperLogLog {

  def apply(log2m: Int): HyperLogLog = new HyperLogLog(log2m, new RegisterSet(1 << log2m)())

  def apply(log2m: Int, register: Array[Int]): HyperLogLog =
    new HyperLogLog(log2m, new RegisterSet(1 << log2m)(register))

  private def getAlphaMM(p: Int, m: Int): Double = {
    // See the paper.
    p match {
      case 4 => 0.673 * m * m
      case 5 => 0.697 * m * m
      case 6 => 0.709 * m * m
      case _ => (0.7213 / (1 + 1.079 / m)) * m * m
    }
  }

  private def linearCounting(m: Int, V: Double): Double = m * math.log(m / V)
}