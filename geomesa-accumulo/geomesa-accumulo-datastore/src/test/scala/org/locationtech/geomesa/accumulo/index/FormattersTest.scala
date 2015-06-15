/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.data.DataUtilities
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FormattersTest extends Specification {
  "PartitionTextFormatter" should {
    val numTrials = 100

    val featureType = SimpleFeatureTypes.createType("TestFeature",
      "the_geom:Point,lat:Double,lon:Double,date:String")
    val featureWithId = DataUtilities.createFeature(
      featureType, "fid1=POINT(-78.1 38.2)|38.2|-78.1|2014-03-20T07:28:00.0Z" )

    val partitionTextFormatter = PartitionTextFormatter(99)

    "map features with non-null identifiers to fixed partitions" in {
      val shardNumbers = (1 to numTrials).map(trial =>
        // The nulls are unused
        partitionTextFormatter.format(null, null, featureWithId)
      ).toSet

      shardNumbers.size must be equalTo 1
    }
  }
}
