/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.OutputStream

import org.geotools.GML
import org.geotools.GML.Version
import org.geotools.data.simple.SimpleFeatureCollection

class GmlExporter(os: OutputStream) extends FeatureExporter {

  val encode = new GML(Version.WFS1_0)
  // JNH: "location" is unlikely to be a valid namespace.
  encode.setNamespace("location", "location.xsd")

  override def export(features: SimpleFeatureCollection): Option[Long] = {
    encode.encode(os, features)
    None
  }

  override def flush(): Unit  = os.flush()

  override def close(): Unit  = {
    os.flush()
    os.close()
  }
}
