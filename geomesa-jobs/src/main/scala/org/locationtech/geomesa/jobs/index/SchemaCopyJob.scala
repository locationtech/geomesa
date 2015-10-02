/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.index

import com.twitter.scalding._
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.jobs.GeoMesaBaseJob
import org.locationtech.geomesa.jobs.scalding.ConnectionParams._
import org.locationtech.geomesa.jobs.scalding._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import scala.collection.JavaConverters._

/**
 * Class to copy a schema and all data from one data store to another.
 *
 * Can be used to 'update' geomesa data from older versions. It does this by reading data in the old format
 * and writing it to a new schema which will use the latest format. This way, improvements in serialization,
 * etc can be leveraged for old data.
 */
class SchemaCopyJob(args: Args) extends GeoMesaBaseJob(args) {

  val featureIn   = args(FEATURE_IN)
  val featureOut  = args.getOrElse(FEATURE_OUT, featureIn)
  val dsInParams  = toDataStoreInParams(args)
  val dsOutParams = toDataStoreOutParams(args)
  val filter      = args.optional(CQL_IN)

  val input = GeoMesaInputOptions(dsInParams, featureIn, filter)
  val output = GeoMesaOutputOptions(dsOutParams)

  @transient lazy val sftIn = {
    val dsIn = DataStoreFinder.getDataStore(dsInParams.asJava)
    require(dsIn != null, "The specified input data store could not be created - check your job parameters")
    val sft = dsIn.getSchema(featureIn)
    require(sft != null, s"The feature '$featureIn' does not exist in the input data store")
    sft
  }
  @transient lazy val sftOut = {
    val dsOut = DataStoreFinder.getDataStore(dsOutParams.asJava)
    require(dsOut != null, "The specified output data store could not be created - check your job parameters")
    var sft = dsOut.getSchema(featureOut)
    if (sft == null) {
      // update the feature name
      if (featureOut == featureIn) {
        sft = sftIn
      } else {
        sft = SimpleFeatureTypes.createType(featureOut, SimpleFeatureTypes.encodeType(sftIn))
      }
      // create the schema in the output datastore
      dsOut.createSchema(sft)
      dsOut.getSchema(featureOut)
    } else {
      sft
    }
  }

  // initialization - ensure the types exist before launching distributed job
  require(sftOut != null, "Could not create output type - check your job parameters")

  // scalding job
  TypedPipe.from(GeoMesaSource(input)).map {
    case (t, sf) => (t, new ScalaSimpleFeature(sf.getID, sftOut, sf.getAttributes.toArray))
  }.write(GeoMesaSource(output))
}
