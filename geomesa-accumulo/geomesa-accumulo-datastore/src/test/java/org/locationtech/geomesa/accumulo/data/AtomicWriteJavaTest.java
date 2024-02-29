/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.FeatureWriter;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.DataUtilities;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.accumulo.data.writer.tx.ConditionalWriteException;
import org.locationtech.geomesa.accumulo.data.writer.tx.ConditionalWriteException.ConditionalWriteStatus;
import org.locationtech.geomesa.index.api.IndexAdapter.IndexWriter;
import org.locationtech.geomesa.index.geotools.AtomicWriteTransaction;
import org.locationtech.geomesa.utils.index.IndexMode;
import scala.Option;

import java.io.IOException;
import java.util.Collections;

/**
 * Note: this test is to ensure that atomic writes are usable in Java without complex workarounds, so
 * the only thing being tested is that it compiles.
 */
public class AtomicWriteJavaTest {

    public void testJavaApi() {
        try {
            DataStore ds = DataStoreFinder.getDataStore(Collections.emptyMap());
            try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                       ds.getFeatureWriter("mysft",
                                           ECQL.toFilter("IN('myid')"),
                                           AtomicWriteTransaction.INSTANCE)) {
                writer.hasNext();
                writer.next();
                writer.write();
            } catch (ConditionalWriteException e) {
                e.getFeatureId();
                for (ConditionalWriteStatus status : e.getRejections()) {
                    status.action();
                    status.condition();
                    status.index();
                }
            }
        } catch (IOException | CQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void testLowLevelJavaApi() {
        try {
            AccumuloDataStore ds = (AccumuloDataStore) DataStoreFinder.getDataStore(Collections.emptyMap());
            SimpleFeatureType sft = ds.getSchema("mysft");
            try (IndexWriter writer = ds.adapter().createWriter(sft,
                                                                ds.manager().indices(sft, IndexMode.Write()),
                                                                Option.empty(),
                                                                true)) {
                writer.update(DataUtilities.createFeature(sft, "update"), DataUtilities.createFeature(sft, "prev"));
            } catch (ConditionalWriteException e) {
                e.getFeatureId();
                for (ConditionalWriteStatus status : e.getRejections()) {
                    status.action();
                    status.condition();
                    status.index();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
