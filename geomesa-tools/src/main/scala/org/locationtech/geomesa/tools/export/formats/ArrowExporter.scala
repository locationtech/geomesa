/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.OutputStream

import org.geotools.data.simple.SimpleFeatureCollection
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileWriter
import org.locationtech.geomesa.utils.collection.SelfClosingIterator

class ArrowExporter(os: OutputStream) extends FeatureExporter {

  import org.locationtech.geomesa.arrow.allocator

  private var writer: SimpleFeatureArrowFileWriter = _

  override def export(fc: SimpleFeatureCollection): Option[Long] = {
    val sft = fc.getSchema
    val features = SelfClosingIterator(fc.features())
    if (sft == org.locationtech.geomesa.arrow.ArrowEncodedSft) {
      // just copy bytes directly out
      features.foreach(f => os.write(f.getAttribute(0).asInstanceOf[Array[Byte]]))
      None // we don't know the actual count
    } else {
      writer = new SimpleFeatureArrowFileWriter(sft, os)
      writer.start()
      var count = 0L
      features.foreach { f =>
        writer.add(f)
        count += 1
      }
      Some(count)
    }
  }

  override def flush(): Unit = {
    if (writer != null) {
      writer.flush()
    }
    os.flush()
  }

  override def close(): Unit = {
    if (writer != null) {
      writer.close()
    }
    os.close()
  }
}
