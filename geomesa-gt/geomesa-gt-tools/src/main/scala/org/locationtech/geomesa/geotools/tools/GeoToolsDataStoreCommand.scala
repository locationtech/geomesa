/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geotools.tools

import java.io.{File, FileInputStream}
import java.util.Properties

import com.beust.jcommander.{Parameter, ParameterException}
import org.geotools.data.DataStore
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand.GeoToolsDataStoreParams
import org.locationtech.geomesa.tools.utils.ParameterConverters.KeyValueConverter
import org.locationtech.geomesa.tools.{DataStoreCommand, DistributedCommand}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

/**
 * Abstract class for commands that have a pre-existing catalog
 */
trait GeoToolsDataStoreCommand extends DataStoreCommand[DataStore] {

  import scala.collection.JavaConverters._

  override def params: GeoToolsDataStoreParams

  override lazy val connection: Map[String, String] = {
    val file = Option(params.paramFile)
    if (params.params.isEmpty && file.isEmpty) {
      throw new ParameterException("Please specify the data store connection through --param and/or --params")
    } else {
      val error = file.collect {
        case f if !f.exists     => s"${f.getAbsolutePath} does not exist"
        case f if !f.canRead    => s"${f.getAbsolutePath} can't be read"
        case f if f.isDirectory => s"${f.getAbsolutePath} is a directory"
      }
      error.foreach(e => throw new ParameterException(s"Invalid parameter file: $e"))
    }
    val props = new Properties()
    file.foreach(f => props.load(new FileInputStream(f)))
    params.params.asScala.foreach { case (k, v) => props.put(k, v) }
    props.asScala.toMap
  }
}

object GeoToolsDataStoreCommand {

  trait GeoToolsDistributedCommand extends GeoToolsDataStoreCommand with DistributedCommand {

    abstract override def libjarsFiles: Seq[String] =
      Seq("org/locationtech/geomesa/geotools/tools/gt-libjars.list") ++ super.libjarsFiles

    abstract override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
      () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_GEOTOOLS_HOME", "lib")
    ) ++ super.libjarsPaths
  }

  trait GeoToolsDataStoreParams {
    @Parameter(
      names = Array("--param"),
      description = "Parameter for DataStoreFinder to load the data store, in the form key=value",
      variableArity = true,
      converter = classOf[KeyValueConverter])
    val params: java.util.List[(String, String)] = new java.util.ArrayList()

    @Parameter(
      names = Array("--params"),
      description = "Java properties file containing parameters for DataStoreFinder to load the data store")
    var paramFile: File = _
  }
}
