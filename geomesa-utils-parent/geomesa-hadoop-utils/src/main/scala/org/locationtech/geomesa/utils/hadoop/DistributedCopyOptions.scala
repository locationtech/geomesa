/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.hadoop

import org.apache.hadoop.fs.Path
import org.apache.hadoop.tools.DistCpOptions

object DistributedCopyOptions {

  import scala.collection.JavaConverters._

  /**
   * Create the options
   *
   * @param sources source files to copy
   * @param dest destination
   * @return
   */
  def apply(sources: Seq[Path], dest: Path): DistCpOptions =
    try { distCpOptions3(Left(sources), dest) } catch { case _: ClassNotFoundException => distCpOptions2(Left(sources), dest) }

  /**
   * Create the options
   *
   * @param sourceFileList file containing list of sources to copy
   * @param dest destination
   * @return
   */
  def apply(sourceFileList: Path, dest: Path): DistCpOptions =
    try { distCpOptions3(Right(sourceFileList), dest) } catch { case _: ClassNotFoundException => distCpOptions2(Right(sourceFileList), dest) }

  // hadoop 3 API
  private def distCpOptions3(sources: Either[Seq[Path], Path], dest: Path): DistCpOptions = {
    val builder = sources match {
      case Right(file) => new DistCpOptions.Builder(file, dest)
      case Left(dirs)  => new DistCpOptions.Builder(dirs.asJava, dest)
    }
    builder.withAppend(false).withOverwrite(true).withBlocking(false).withCopyStrategy("dynamic").build()
  }

  // hadoop 2 API
  private def distCpOptions2(sources: Either[Seq[Path], Path], dest: Path): DistCpOptions = {
    val clas = classOf[DistCpOptions]
    val opts = sources match {
      case Right(file) => clas.getConstructor(classOf[Path], classOf[Path]).newInstance(file, dest)
      case Left(dirs)  => clas.getConstructor(classOf[java.util.List[Path]], classOf[Path]).newInstance(dirs.asJava, dest)
    }
    clas.getMethod("setAppend", classOf[Boolean]).invoke(opts, java.lang.Boolean.FALSE)
    clas.getMethod("setOverwrite", classOf[Boolean]).invoke(opts, java.lang.Boolean.TRUE)
    clas.getMethod("setCopyStrategy", classOf[String]).invoke(opts, "dynamic")
    opts
  }
}
