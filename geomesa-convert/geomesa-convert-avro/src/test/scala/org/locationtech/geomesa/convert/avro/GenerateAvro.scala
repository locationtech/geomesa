/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import java.io.{FileOutputStream, File}

import org.apache.avro.file.DataFileWriter

// helper for integration tests and such
object GenerateAvro extends AvroUtils {

  def main(args: Array[String]): Unit = {
    val f = new File("/tmp/no-header.avro")
    f.createNewFile()
    val fos = new FileOutputStream(f)
    fos.write(bytes)
    fos.write(bytes2)
    fos.close()

    val dfw = new DataFileWriter(writer)
    dfw.create(schema, new File("/tmp/with-header.avro"))
    dfw.append(obj)
    dfw.append(obj2)
    dfw.close()
  }

}
