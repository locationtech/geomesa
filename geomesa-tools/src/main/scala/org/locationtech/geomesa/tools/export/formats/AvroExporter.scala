/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.OutputStream

import org.geotools.data.simple.SimpleFeatureCollection
import org.locationtech.geomesa.features.avro.AvroDataFileWriter
import org.opengis.feature.simple.SimpleFeatureType

class AvroExporter(sft: SimpleFeatureType, os: OutputStream, compression: Int) extends FeatureExporter {

  val writer = new AvroDataFileWriter(os, sft, compression)

  override def export(fc: SimpleFeatureCollection): Option[Long] = {
    writer.append(fc)
    None
  }

  override def flush(): Unit = {
    writer.flush()
    os.flush()
  }

  override def close(): Unit = {
    writer.close()
    os.close()
  }
}
