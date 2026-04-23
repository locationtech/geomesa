/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io.s3

import org.apache.parquet.io.{OutputFile, PositionOutputStream}
import org.locationtech.geomesa.fs.storage.core.fs.S3ObjectStore
import org.locationtech.geomesa.fs.storage.parquet.io.s3.S3OutputFile.S3PositionOutputStream

import java.io.{IOException, OutputStream}
import java.net.URI

/**
 * S3 parquet output file
 *
 * @param fs file system
 * @param path file path
 */
class S3OutputFile(fs: S3ObjectStore, path: URI) extends OutputFile {

  override def create(blockSizeHint: Long): PositionOutputStream =
    new S3PositionOutputStream(fs.create(path).getOrElse(throw new IOException(s"File exists at $path")))

  override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream = new S3PositionOutputStream(fs.overwrite(path))

  override def supportsBlockSize(): Boolean = false

  override def defaultBlockSize(): Long = -1L
}

object S3OutputFile {

  private class S3PositionOutputStream(os: OutputStream) extends PositionOutputStream {

    private var pos = 0

    override def getPos: Long = pos

    override def write(data: Int): Unit = {
      pos += 1
      os.write(data)
    }

    override def write(data: Array[Byte]): Unit = {
      pos += data.length
      os.write(data)
    }

    override def write(data: Array[Byte], off: Int, len: Int): Unit = {
      pos += len
      os.write(data, off, len)
    }

    override def flush(): Unit = os.flush()

    override def close(): Unit = os.close()
  }
}
