/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.avro

import java.io._

import org.apache.avro.file.DataFileWriter
import org.geotools.data.simple.SimpleFeatureCollection
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.utils.geotools.Conversions
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Class that writes out Avro SimpleFeature Data Files which carry the SimpleFeatureType Schema
  * along with them.
  *
  * @param os output stream.
  */
class AvroDataFileWriter(os: OutputStream, sft: SimpleFeatureType) extends Closeable with Flushable {

  private val schema = AvroSimpleFeatureUtils.generateSchema(sft, true)
  private val dfw = new DataFileWriter[SimpleFeature](new AvroSimpleFeatureWriter(sft, SerializationOptions.withUserData))
  AvroDataFile.setMetaData(dfw, sft)
  dfw.create(schema, os)

  def append(fc: SimpleFeatureCollection): Unit = {
    import Conversions._
    fc.features().foreach(dfw.append)
  }

  def append(sf: SimpleFeature): Unit =  dfw.append(sf)

  override def close(): Unit = if (dfw != null) dfw.close()

  override def flush(): Unit = if (dfw != null) dfw.flush()
}
