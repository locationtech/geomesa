/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.process

import org.geotools.api.data.Query
import org.locationtech.geomesa.index.stats.Stat
import org.locationtech.geomesa.index.view.MergedDataStoreViewTestSupport
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.specs2.mutable.SpecificationWithJUnit

class MinMaxVisitorTest extends SpecificationWithJUnit with MergedDataStoreViewTestSupport {
  "MinMaxVisitor" should {
    "preserve fallbacks for ordinary feature sources" in {
      foreach(Seq(new Store, new ExactStore)) { store =>
        try {
          populate(store, "source")
          val source = store.getFeatureSource(typeName)
          val features = source.getFeatures()
          val expected = Stat(source.getSchema, Stat.MinMax("dtg"))
          CloseableIterator(features.features()).foreach(expected.observe)
          val visitor = new MinMaxVisitor(features, "dtg", cached = false)
          visitor.execute(source, new Query(typeName))
          val result = visitor.getResult.results.features()
          try { result.next().getAttribute(0).toString mustEqual expected.toJson }
          finally { result.close() }
        } finally { store.dispose() }
      }
    }
  }
}
