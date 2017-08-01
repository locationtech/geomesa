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
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder

class GmlExporter(os: OutputStream) extends FeatureExporter {

  override def export(features: SimpleFeatureCollection): Option[Long] = {

    val encode = new GML(Version.WFS1_0)
    encode.setNamespace("geomesa", "http://geomesa.org")

    val fcToWrite = if (features.getSchema.getName.getNamespaceURI == null) {
      val namespacedSFT = {
        val builder = new SimpleFeatureTypeBuilder()
        builder.init(features.getSchema)
        builder.setNamespaceURI("http://geomesa.org")
        builder.buildFeatureType()
      }

      new ReTypingFeatureCollection(features, namespacedSFT)
    } else {
      features
    }

    encode.encode(os, fcToWrite)
    None
  }

  override def close(): Unit  = {
    os.flush()
    os.close()
  }
}
