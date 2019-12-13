/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.stats

import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.opengis.filter.Filter
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatsCombinerTest extends TestWithDataStore {

  import scala.collection.JavaConverters._

  sequential

  override val spec = "name:String,dtg:Date,*geom:Point:srid=4326"

  step {
    // add two batches so that we have multiple rows to combine in the stats iter
    addFeatures(Seq(ScalaSimpleFeature.create(sft, "0", "name0", "2017-01-01T00:00:00.000Z", "POINT (40 55)")))
    addFeatures(Seq(ScalaSimpleFeature.create(sft, "1", "name1", "2017-01-01T01:00:00.000Z", "POINT (41 55)")))
  }

  // gets a new data store so that we don't read any cached values
  def statCount(): Option[Long] = {
    val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[AccumuloDataStore]
    try { ds.stats.getCount(sft, Filter.INCLUDE, exact = false) } finally { ds.dispose() }
  }

  "StatsCombiner" should {
    "add/remove configured combiners" in {
      statCount() must beSome(2L)
      ds.stats.removeStatCombiner(ds.connector, sft)
      // the exact behavior here doesn't matter, it's just to verify that the combiner is not enabled
      // in this case, it will just return the first row
      statCount() must beSome(1L)
      ds.stats.configureStatCombiner(ds.connector, sft)
      statCount() must beSome(2L)
    }
  }
}
