/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import org.opengis.feature.simple.SimpleFeatureType

import java.io.OutputStream
import java.util.zip.Deflater

@deprecated("Moved to org.locationtech.geomesa.features.avro.io.AvroDataFileWriter")
class AvroDataFileWriter(os: OutputStream, sft: SimpleFeatureType, compression: Int = Deflater.DEFAULT_COMPRESSION)
  extends org.locationtech.geomesa.features.avro.io.AvroDataFileWriter(os, sft, compression)
