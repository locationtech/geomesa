/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.io

import org.apache.parquet.io.{OutputFile, PositionOutputStream}
import org.locationtech.geomesa.fs.storage.core.parquet.io.IcebergOutputFile.PositionOutputStreamWrapper

/**
 * Parquet output file that wraps an iceberg output file
 *
 * @param original iceberg output file
 */
class IcebergOutputFile(val original: org.apache.iceberg.io.OutputFile) extends OutputFile {

  override def create(blockSizeHint: Long): PositionOutputStream = new PositionOutputStreamWrapper(original.create())

  override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream =
    new PositionOutputStreamWrapper(original.createOrOverwrite())

  override def supportsBlockSize(): Boolean = false

  override def defaultBlockSize(): Long = -1L
}

object IcebergOutputFile {

  private class PositionOutputStreamWrapper(os: org.apache.iceberg.io.PositionOutputStream) extends PositionOutputStream {
    override def getPos: Long = os.getPos
    override def write(b: Int): Unit = os.write(b)
    override def write(data: Array[Byte]): Unit = os.write(data)
    override def write(data: Array[Byte], off: Int, len: Int): Unit = os.write(data, off, len)
    override def flush(): Unit = os.flush()
    override def close(): Unit = os.close()
  }
}
