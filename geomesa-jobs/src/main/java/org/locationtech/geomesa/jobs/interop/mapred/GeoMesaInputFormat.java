/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.interop.mapred;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.geotools.data.Query;
import org.locationtech.geomesa.jobs.mapred.GeoMesaInputFormat$;
import org.opengis.feature.simple.SimpleFeature;
import scala.Option;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.Map;

/**
 * Input format that will read simple features from GeoMesa based on a CQL query.
 * The key will be the feature ID. Configure using the static methods.
 */
public class GeoMesaInputFormat implements InputFormat<Text, SimpleFeature> {

    private org.locationtech.geomesa.jobs.mapred.GeoMesaInputFormat delegate =
            new org.locationtech.geomesa.jobs.mapred.GeoMesaInputFormat();

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        return delegate.getSplits(job, numSplits);
    }

    @Override
    public RecordReader<Text, SimpleFeature> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
            throws IOException {
        return delegate.getRecordReader(split, job, reporter);
    }

    public static void configure(JobConf job, Map<String, String> dataStoreParams, Query query) {
        scala.collection.immutable.Map<String, String> scalaParams =
                JavaConverters.asScalaMapConverter(dataStoreParams).asScala()
                              .toMap(Predef.<Tuple2<String, String>>conforms());
        GeoMesaInputFormat$.MODULE$.configure(job, scalaParams, query);
    }

    @Deprecated
    public static void configure(JobConf job,
                                 Map<String, String> dataStoreParams,
                                 String featureTypeName,
                                 String filter,
                                 String[] transform) {
        scala.collection.immutable.Map<String, String> scalaParams =
                JavaConverters.asScalaMapConverter(dataStoreParams).asScala()
                              .toMap(Predef.<Tuple2<String, String>>conforms());
        Option<String> f = Option.apply(filter);
        Option<String[]> t = Option.apply(transform);
        GeoMesaInputFormat$.MODULE$.configure(job, scalaParams, featureTypeName, f, t);
    }
}