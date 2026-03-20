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

  private final val SafeNameRegex = "[^a-zA-Z0-9_-]+".r

  /**
    * Get the path for a new data file
    *
    * @param ext file extension
    * @param fileType file type
    * @return
    */
  def nextFile(typeName: String, fileType: FileType.FileType, ext: String): String = {
    val filename =
      s"${fileType}_${SafeNameRegex.replaceAllIn(typeName, "-").take(20)}_${UUID.randomUUID().toString.replaceAllLiterally("-", "")}.$ext"
    // partitioning logic taken from Apache Iceberg: https://iceberg.apache.org/docs/nightly/aws/#object-store-file-layout
    val hash = {
      val bytes = filename.getBytes(StandardCharsets.UTF_8)
      val hash = MurmurHash3.hash32x86(bytes, 0, bytes.length, 0)
      // Integer#toBinaryString excludes leading zeros, which we want to preserve
      Integer.toBinaryString(hash | Integer.MIN_VALUE)
    }
    s"${hash.substring(0, 4)}/${hash.substring(4, 8)}/${hash.substring(8, 12)}/${hash.substring(12, 20)}/$filename"
  }

  object FileType extends Enumeration {
    type FileType = Value
    val Written  : Value = Value("w")
    val Compacted: Value = Value("c")
    val Imported : Value = Value("i")
    val Modified : Value = Value("m")
    val Deleted  : Value = Value("d")
  }
}
