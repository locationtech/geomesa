/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.OutputStream
import java.util.zip.Deflater

import org.locationtech.geomesa.features.avro.AvroDataFileWriter
import org.locationtech.geomesa.tools.export.formats.FeatureExporter.{ByteCounter, ByteCounterExporter}
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class AvroExporter(os: OutputStream, counter: ByteCounter, compression: Option[Int])
    extends ByteCounterExporter(counter) {

  private var writer: AvroDataFileWriter = _

  override def start(sft: SimpleFeatureType): Unit =
    writer = new AvroDataFileWriter(os, sft, compression.getOrElse(Deflater.DEFAULT_COMPRESSION))

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    var count = 0L
    features.foreach { feature =>
      writer.append(feature)
      count += 1L
    }
    writer.flush()
    Some(count)
  }

  override def close(): Unit = {
    CloseWithLogging(Option(writer))
    os.close()
  }
}
