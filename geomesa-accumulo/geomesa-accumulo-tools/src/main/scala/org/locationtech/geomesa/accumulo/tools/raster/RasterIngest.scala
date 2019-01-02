/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.raster

import java.io.{File, Serializable}
import java.util.{Map => JMap}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.coverage.grid.io.{AbstractGridCoverage2DReader, GridFormatFinder}
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStoreParams => dsp}
import org.locationtech.geomesa.raster.data.AccumuloRasterStore
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams
import org.locationtech.geomesa.raster.{RasterParams => rsp}

import scala.collection.JavaConversions._

trait RasterIngest extends LazyLogging {
  def getAccumuloRasterStoreConf(config: Map[String, Option[String]]): JMap[String, Serializable] =
    mapAsJavaMap(Map(
      dsp.InstanceIdParam.getName   -> config(IngestRasterParams.ACCUMULO_INSTANCE).get,
      dsp.ZookeepersParam.getName   -> config(IngestRasterParams.ZOOKEEPERS).get,
      dsp.UserParam.getName         -> config(IngestRasterParams.ACCUMULO_USER).get,
      dsp.PasswordParam.getName     -> config(IngestRasterParams.ACCUMULO_PASSWORD).get,
      dsp.CatalogParam.getName      -> config(IngestRasterParams.TABLE).get,
      dsp.AuthsParam.getName        -> config(IngestRasterParams.AUTHORIZATIONS),
      dsp.VisibilitiesParam.getName -> config(IngestRasterParams.VISIBILITIES),
      rsp.writeMemoryParam.getName  -> config(IngestRasterParams.WRITE_MEMORY),
      dsp.WriteThreadsParam         -> config(IngestRasterParams.WRITE_THREADS),
      dsp.QueryThreadsParam.getName -> config(IngestRasterParams.QUERY_THREADS),
      dsp.MockParam.getName         -> config(IngestRasterParams.ACCUMULO_MOCK)
    ).collect {
      case (key, Some(value)) => (key, value);
      case (key, value: String) => (key, value)
    }).asInstanceOf[java.util.Map[String, Serializable]]

  def createRasterStore(config: Map[String, Option[String]]): AccumuloRasterStore = {
    val rasterName = config(IngestRasterParams.TABLE)
    if (rasterName == null || rasterName.isEmpty) {
      logger.error("No raster name specified for raster feature ingest." +
        " Please check that all arguments are correct in the previous command. ")
      sys.exit()
    }

    val csConfig: JMap[String, Serializable] = getAccumuloRasterStoreConf(config)

    AccumuloRasterStore(csConfig)
  }

  def getReader(imageFile: File, imageType: String): AbstractGridCoverage2DReader = {
    val format = GridFormatFinder.findFormat(imageFile)
    if (format == null) {
      throw new Exception(s"Image format for file: ${imageFile.getName} is not supported.")
    }
    val reader = format.getReader(imageFile)
    if (reader == null) {
      throw new Exception("Could not instantiate reader for format of file: ${imageFile.getName}")
    }
    reader
  }

  val defaultHints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)
}
