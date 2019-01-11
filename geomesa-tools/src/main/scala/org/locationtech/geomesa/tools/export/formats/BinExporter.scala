/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.OutputStream

import org.geotools.factory.Hints
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class BinExporter(hints: Hints, os: OutputStream) extends FeatureExporter {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  private var encoder: Option[BinaryOutputEncoder] = None

  override def start(sft: SimpleFeatureType): Unit = {
    if (sft == BinaryOutputEncoder.BinEncodedSft) {
      encoder = None
    } else {
      import org.locationtech.geomesa.index.conf.QueryHints.RichHints
      // do the encoding here
      val geom = hints.getBinGeomField.map(sft.indexOf)
      val dtg = hints.getBinDtgField.map(sft.indexOf)
      val track = Option(hints.getBinTrackIdField).filter(_ != "id").map(sft.indexOf)
      val options = EncodingOptions(geom, dtg, track, hints.getBinLabelField.map(sft.indexOf))
      encoder = Some(BinaryOutputEncoder(sft, options))
    }
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    val count = encoder match {
      case Some(e) => e.encode(features, os)
      case None =>
        var numBytes = 0L
        // just copy bytes directly out
        features.foreach { f =>
          val bytes = f.getAttribute(0).asInstanceOf[Array[Byte]]
          os.write(bytes)
          numBytes += bytes.length
        }
        numBytes / (if (hints.getBinLabelField.isEmpty) { 16 } else { 24 })
    }
    os.flush()
    Some(count)
  }

  override def close(): Unit = os.close()
}
