/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.OutputStream

import org.geotools.GML
import org.geotools.GML.Version
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class GmlExporter(os: OutputStream) extends FeatureExporter {

  private val encode = new GML(Version.WFS1_0)
  encode.setNamespace("geomesa", "http://geomesa.org")

  private var sft: SimpleFeatureType = _
  private var retyped: Option[SimpleFeatureType] = _

  override def start(sft: SimpleFeatureType): Unit = {
    this.sft = sft
    this.retyped = if (sft.getName.getNamespaceURI != null) { None } else {
      val builder = new SimpleFeatureTypeBuilder()
      builder.init(sft)
      builder.setNamespaceURI("http://geomesa.org")
      Some(builder.buildFeatureType())
    }
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    val array = features.toArray
    val collection = {
      val list = new ListFeatureCollection(sft, array)
      retyped.map(r => new ReTypingFeatureCollection(list, r)).getOrElse(list)
    }
    encode.encode(os, collection)
    os.flush()
    Some(array.length.toLong)
  }

  override def close(): Unit = os.close()
}
