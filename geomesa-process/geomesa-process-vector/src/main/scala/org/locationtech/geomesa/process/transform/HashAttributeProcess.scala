/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.transform

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.process.GeoMesaProcess
import org.locationtech.geomesa.utils.collection.SelfClosingIterator

trait HashAttribute {

  private val hashFn = Hashing.goodFastHash(64)

  def transformHash(hash: Int): AnyRef
  // note - augmentSft needs to add an attribute called 'hash'
  def augmentSft(sft: SimpleFeatureTypeBuilder): Unit

  @throws(classOf[ProcessException])
  @DescribeResult(name = "result", description = "Output collection")
  def execute(@DescribeParameter(name = "data", description = "Input features")
              obsFeatures: SimpleFeatureCollection,
              @DescribeParameter(name = "attribute", description = "The attribute to hash on")
              attribute: String,
              @DescribeParameter(name = "modulo", description = "The divisor")
              modulo: Integer): SimpleFeatureCollection = {

    val sft = obsFeatures.getSchema
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.init(sft)
    augmentSft(sftBuilder)
    val targetSft = sftBuilder.buildFeatureType()
    val hashIndex = targetSft.indexOf("hash")
    val featureBuilder = new SimpleFeatureBuilder(targetSft)

    val results = new ListFeatureCollection(targetSft)

    SelfClosingIterator(obsFeatures.features()).foreach { sf =>
      featureBuilder.reset()
      featureBuilder.init(sf)
      val attr = Option(sf.getAttribute(attribute)).map(_.toString).getOrElse("")
      val hash = math.abs(hashFn.hashString(attr, Charsets.UTF_16LE).asInt()) % modulo
      featureBuilder.set(hashIndex, transformHash(hash))
      results.add(featureBuilder.buildFeature(sf.getID))
    }

    results
  }
}

@DescribeProcess(
  title = "Hash Attribute Process",
  description = "Adds an attribute to each SimpleFeature that hashes the configured attribute modulo the configured param"
)
class HashAttributeProcess extends GeoMesaProcess with HashAttribute {
  override def transformHash(hash: Int): AnyRef = Int.box(hash)

  override def augmentSft(sftBuilder: SimpleFeatureTypeBuilder): Unit = {
    sftBuilder.add("hash", classOf[Integer])
  }
}

@DescribeProcess(
  title = "Hash Attribute Color Process",
  description = "Adds an attribute to each SimpleFeature that hashes the configured attribute modulo the configured param and emits a color"
)
class HashAttributeColorProcess extends GeoMesaProcess with HashAttribute {
  val colors =
    Array[String](
      "#6495ED",
      "#B0C4DE",
      "#00FFFF",
      "#9ACD32",
      "#00FA9A",
      "#FFF8DC",
      "#F5DEB3")

  override def transformHash(hash: Int): AnyRef = colors(hash % colors.length)

  override def augmentSft(sftBuilder: SimpleFeatureTypeBuilder): Unit = {
    sftBuilder.add("hash", classOf[String])
  }
}