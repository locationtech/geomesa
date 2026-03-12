/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.utils

import org.apache.commons.codec.digest.MurmurHash3

import java.nio.charset.StandardCharsets
import java.util.UUID

object StorageUtils {

  /**
    * Get the path for a new data file
    *
    * @param extension file extension
    * @param fileType file type
    * @return
    */
  def nextFile(
      extension: String,
      fileType: FileType.FileType,
      name: String = UUID.randomUUID().toString.replaceAllLiterally("-", "")): String = {
    // partitioning logic taken from Apache Iceberg: https://iceberg.apache.org/docs/nightly/aws/#object-store-file-layout
    val filename = s"$fileType$name.$extension"
    val hash = {
      val bytes = filename.getBytes(StandardCharsets.UTF_8)
      val hash = MurmurHash3.hash32x86(bytes, 0, bytes.length, 0)
      // Integer#toBinaryString excludes leading zeros, which we want to preserve
      Integer.toBinaryString(hash | Integer.MIN_VALUE)
    }
    s"/data/${hash.substring(0, 4)}/${hash.substring(4, 8)}/${hash.substring(8, 12)}/${hash.substring(12, 20)}/$filename"
  }

  object FileType extends Enumeration {
    type FileType = Value
    val Written  : Value = Value("W")
    val Compacted: Value = Value("C")
    val Imported : Value = Value("I")
    val Modified : Value = Value("M")
    val Deleted  : Value = Value("D")
  }
}
