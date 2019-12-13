/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.function

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.expression.VolatileFunction

import scala.util.hashing.MurmurHash3

class ProxyIdFunction extends FunctionExpressionImpl(ProxyIdFunction.Name) with VolatileFunction with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override def evaluate(obj: AnyRef): Integer = obj match {
    case null => null

    case sf: SimpleFeature =>
      if (sf.getFeatureType.isUuid) {
        val uuid = sf.getUuid
        Int.box(ProxyIdFunction.proxyId(uuid._1, uuid._2))
      } else {
        Int.box(ProxyIdFunction.proxyId(sf.getID))
      }

    case _ =>
      logger.warn(s"Unhandled input: $obj")
      null
  }
}

object ProxyIdFunction {

  val Name = new FunctionNameImpl("proxyId", classOf[Int])

  /**
    * Proxy a UUID
    *
    * @param msb most significant bits
    * @param lsb least significant bits
    * @return
    */
  def proxyId(msb: Long, lsb: Long): Int = {
    // seed 0 murmur hash of 4 ints comprising the uuid
    val m1 = MurmurHash3.mix(0, msb.toInt)
    val m2 = MurmurHash3.mix(m1, (msb >>> 32).toInt)
    val m3 = MurmurHash3.mix(m2, lsb.toInt)
    val m4 = MurmurHash3.mixLast(m3, (lsb >>> 32).toInt)
    MurmurHash3.finalizeHash(m4, 16)
  }

  /**
    * Proxy a string
    *
    * @param id feature id
    * @return
    */
  def proxyId(id: String): Int = MurmurHash3.stringHash(id)
}
