/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io.fs

import org.apache.commons.io.IOUtils
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.{CachingStdInHandle, StdInHandle}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.{BufferedInputStream, ByteArrayInputStream, IOException, InputStream}

@RunWith(classOf[JUnitRunner])
class LocalFileHandleTest extends Specification {

  "LocalFileHandle" should {
    "cache bytes when reading from stdin" in {
      @throws[IOException]
      def readNBytes(stream: InputStream, n: Int): Array[Byte] = {
        val buffer = new Array[Byte](n)
        var totalBytesRead = 0

        while (totalBytesRead < n) {
          val bytesRead = stream.read(buffer, totalBytesRead, n - totalBytesRead)
          if (bytesRead == -1) {
            throw new IOException("End of stream reached before reading 'n' bytes.")
          }
          totalBytesRead += bytesRead
        }

        buffer
      }

      val dataFile = WithClose(getClass.getClassLoader.getResourceAsStream("geomesa-fake.xml")) { in =>
        IOUtils.toByteArray(in)
      }

      // Feed all the bytes to stdin
      val input = new BufferedInputStream(new ByteArrayInputStream(dataFile))
      StdInHandle.SystemIns.set(input)
      try {
        val handle = CachingStdInHandle.get()
        handle.length mustEqual dataFile.length
        val List(stream1) = SelfClosingIterator(handle.open).map(_._2).toList
        val first3Bytes = try { readNBytes(stream1, 3) } finally {
          stream1.close()
        }
        handle.length mustEqual dataFile.length
        val List(stream2) = SelfClosingIterator(handle.open).map(_._2).toList
        try { readNBytes(stream2, 3) mustEqual first3Bytes } finally {
          stream2.close()
        }
        val List(stream3) = SelfClosingIterator(handle.open).map(_._2).toList
        try { readNBytes(stream3, 6).take(3) mustEqual first3Bytes } finally {
          stream3.close()
        }
      } finally {
        StdInHandle.SystemIns.remove()
      }
    }
  }
}
