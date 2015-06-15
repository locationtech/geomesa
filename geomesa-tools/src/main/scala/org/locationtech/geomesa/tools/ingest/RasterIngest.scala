/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.ingest

import java.io.{File, Serializable}
import java.util.{Map => JMap}

import com.twitter.scalding.Args
import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader
import org.geotools.coverageio.gdal.dted.DTEDReader
import org.geotools.factory.Hints
import org.geotools.gce.geotiff.GeoTiffReader
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.{params => dsp}
import org.locationtech.geomesa.raster.data.AccumuloRasterStore
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams
import org.locationtech.geomesa.raster.{RasterParams => rsp}
import org.locationtech.geomesa.tools.Utils.Formats._

import scala.collection.JavaConversions._

trait RasterIngest extends Logging {
  def getAccumuloRasterStoreConf(config: Map[String, Option[String]]): JMap[String, Serializable] =
    mapAsJavaMap(Map(
      dsp.instanceIdParam.getName   -> config(IngestRasterParams.ACCUMULO_INSTANCE).get,
      dsp.zookeepersParam.getName   -> config(IngestRasterParams.ZOOKEEPERS).get,
      dsp.userParam.getName         -> config(IngestRasterParams.ACCUMULO_USER).get,
      dsp.passwordParam.getName     -> config(IngestRasterParams.ACCUMULO_PASSWORD).get,
      dsp.tableNameParam.getName    -> config(IngestRasterParams.TABLE).get,
      dsp.authsParam.getName        -> config(IngestRasterParams.AUTHORIZATIONS),
      dsp.visibilityParam.getName   -> config(IngestRasterParams.VISIBILITIES),
      rsp.writeMemoryParam.getName  -> config(IngestRasterParams.WRITE_MEMORY),
      dsp.writeThreadsParam         -> config(IngestRasterParams.WRITE_THREADS),
      dsp.queryThreadsParam.getName -> config(IngestRasterParams.QUERY_THREADS),
      dsp.mockParam.getName         -> config(IngestRasterParams.ACCUMULO_MOCK)
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

  def createRasterStore(args: Args): AccumuloRasterStore = {
    val dsConfig: Map[String, Option[String]] =
      Map(
        IngestRasterParams.ZOOKEEPERS        -> args.optional(IngestRasterParams.ZOOKEEPERS),
        IngestRasterParams.ACCUMULO_INSTANCE -> args.optional(IngestRasterParams.ACCUMULO_INSTANCE),
        IngestRasterParams.ACCUMULO_USER     -> args.optional(IngestRasterParams.ACCUMULO_USER),
        IngestRasterParams.ACCUMULO_PASSWORD -> args.optional(IngestRasterParams.ACCUMULO_PASSWORD),
        IngestRasterParams.TABLE             -> args.optional(IngestRasterParams.TABLE),
        IngestRasterParams.AUTHORIZATIONS    -> args.optional(IngestRasterParams.AUTHORIZATIONS),
        IngestRasterParams.VISIBILITIES      -> args.optional(IngestRasterParams.VISIBILITIES),
        IngestRasterParams.WRITE_MEMORY      -> args.optional(IngestRasterParams.WRITE_MEMORY),
        IngestRasterParams.WRITE_THREADS     -> args.optional(IngestRasterParams.WRITE_THREADS),
        IngestRasterParams.QUERY_THREADS     -> args.optional(IngestRasterParams.QUERY_THREADS),
        IngestRasterParams.ACCUMULO_MOCK     -> args.optional(IngestRasterParams.ACCUMULO_MOCK)
      )
    createRasterStore(dsConfig)
  }

  def getReader(imageFile: File, imageType: String): AbstractGridCoverage2DReader = {
    imageType match {
      case TIFF => new GeoTiffReader(imageFile, defaultHints)
      case DTED => new DTEDReader(imageFile, defaultHints)
      case _    => throw new Exception("Image type is not supported.")
    }
  }

  val defaultHints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)
}
