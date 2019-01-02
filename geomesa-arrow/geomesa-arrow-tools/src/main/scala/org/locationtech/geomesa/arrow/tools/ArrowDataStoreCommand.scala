/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.tools

import java.io.File
import java.net.{MalformedURLException, URL}

import com.beust.jcommander.{Parameter, ParameterException}
import org.locationtech.geomesa.arrow.data.{ArrowDataStore, ArrowDataStoreFactory}
import org.locationtech.geomesa.tools.DataStoreCommand

/**
 * Abstract class for commands that have a pre-existing catalog
 */
trait ArrowDataStoreCommand extends DataStoreCommand[ArrowDataStore] {

  override def params: UrlParam

  override def connection: Map[String, String] = {
    val url = if (params.url.matches("""\w+://.*""")) {
      try {
        new URL(params.url)
      } catch {
        case e: MalformedURLException => throw new ParameterException(s"Invalid URL ${params.url}: ", e)
      }
    } else {
      try {
        new File(params.url).toURI.toURL
      } catch {
        case e: MalformedURLException => throw new ParameterException(s"Invalid URL ${params.url}: ", e)
      }
    }
    Map(ArrowDataStoreFactory.UrlParam.getName -> url.toString)
  }
}

trait UrlParam {
  @Parameter(names = Array("--url", "-u"), description = "URL for an Arrow resource, or path to an arrow file", required = true)
  var url: String = _
}