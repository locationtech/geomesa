/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.uuid

import java.util.{Date, UUID}

import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.curve.{BinnedTime, Z3SFC}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.hashing.MurmurHash3

/**
 * Creates feature id based on the z3 index.
 */
class Z3FeatureIdGenerator extends FeatureIdGenerator {
  override def createId(sft: SimpleFeatureType, sf: SimpleFeature): String = {
    if (sft.getGeometryDescriptor == null) {
      // no geometry in this feature type - just use a random UUID
      UUID.randomUUID().toString
    } else {
      Z3UuidGenerator.createUuid(sft, sf).toString
    }
  }
}

/**
 * UUID generator that creates UUIDs that sort by z3 index.
 * UUIDs will be prefixed with a shard number, which will ensure some distribution of values as well
 * as allow pre-splitting of tables based on hex values.
 *
 * Uses variant 2 (IETF) and version 4 (for random UUIDs, although it's not totally random).
 * See https://en.wikipedia.org/wiki/Universally_unique_identifier#Variants_and_versions
 *
 * Format is:
 *
 *   4 bits for a shard - enough for a single hex digit
 *   44 bits of the z3 index value
 *   4 bits for the UUID version
 *   12 more bits of the z3 index value
 *   2 bits for the UUID variant
 *   62 bits of randomness
 */
object Z3UuidGenerator extends RandomLsbUuidGenerator with LazyLogging {

  /**
   * Creates a UUID where the first 8 bytes are based on the z3 index of the feature and
   * the second 8 bytes are based on a random number.
   *
   * This provides uniqueness along with locality.
   */
  def createUuid(sft: SimpleFeatureType, sf: SimpleFeature): UUID = {
    val time = sft.getDtgIndex.flatMap(i => Option(sf.getAttribute(i)).map(_.asInstanceOf[Date].getTime))
        .getOrElse(System.currentTimeMillis())

    val pt = sf.getAttribute(sft.getGeomIndex)
    validateGeometry(pt.asInstanceOf[Geometry])

    if (sft.isPoints) {
      createUuid(pt.asInstanceOf[Point], time, sft.getZ3Interval)
    } else {
      import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
      createUuid(pt.asInstanceOf[Geometry].safeCentroid(), time, sft.getZ3Interval)
    }
  }

  def createUuid(geom: Geometry, time: Long, period: TimePeriod): UUID = {
    validateGeometry(geom)

    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    createUuid(geom.safeCentroid(), time, period)
  }

  def createUuid(pt: Point, time: Long, period: TimePeriod): UUID = {
    validateGeometry(pt)

    // create the random part
    // this uses the same temp array we use later, so be careful with the order this gets called
    val leastSigBits = createRandomLsb()

    val z3 = {
      val BinnedTime(b, t) = BinnedTime.timeToBinnedTime(period)(time)
      val z = Z3SFC(period).index(pt.getX, pt.getY, t).z
      Bytes.concat(Shorts.toByteArray(b), Longs.toByteArray(z))
    }

    // shard is first 4 bits of our uuid (e.g. 1 hex char) - this allows nice pre-splitting
    val shard = math.abs(MurmurHash3.bytesHash(z3) % 16).toByte

    val msb = getTempByteArray
    // set the shard bits, then the z3 bits
    msb(0) = lohi(shard, z3(0))
    msb(1) = lohi(z3(0), z3(1))
    msb(2) = lohi(z3(1), z3(2))
    msb(3) = lohi(z3(2), z3(3))
    msb(4) = lohi(z3(3), z3(4))
    msb(5) = lohi(z3(4), z3(5))
    msb(6) = lohi(0, (z3(5) << 4).asInstanceOf[Byte]) // leave 4 bits for the version
    msb(7) = z3(6)
    // we drop the last 4 bytes of the z3 to ensure some randomness
    // that leaves us 62 bits of randomness, and still gives us ~10 bits per dimension for locality

    // set the UUID version - we skipped those bits when writing
    setVersion(msb)
    // create the long
    val mostSigBits = Longs.fromByteArray(msb)

    new UUID(mostSigBits, leastSigBits)
  }

  // features with a null geometry-to-index should be rejected
  private def validateGeometry(geom: Geometry): Unit = if (geom == null) {
    throw new IllegalArgumentException("Cannot meaningfully index a feature with a NULL geometry")
  }

  // takes 4 low bits from b1 and 4 high bits of b2 as a new byte
  private def lohi(b1: Byte, b2: Byte) = ((b1 << 4) | (b2 >>> 4)).asInstanceOf[Byte]
}
