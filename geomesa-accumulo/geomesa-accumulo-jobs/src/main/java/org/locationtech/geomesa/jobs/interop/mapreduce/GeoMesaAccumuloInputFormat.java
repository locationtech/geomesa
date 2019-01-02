/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.interop.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.geotools.data.Query;
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaAccumuloInputFormat$;
import org.opengis.feature.simple.SimpleFeature;
import scala.Option;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Input format that will read simple features from GeoMesa based on a CQL query.
 * The key will be the feature ID. Configure using the static methods.
 */
public class GeoMesaAccumuloInputFormat extends InputFormat<Text, SimpleFeature> {

    private org.locationtech.geomesa.jobs.mapreduce.GeoMesaAccumuloInputFormat delegate =
            new org.locationtech.geomesa.jobs.mapreduce.GeoMesaAccumuloInputFormat();

    @Override
    public List<InputSplit> getSplits(JobContext context)
            throws IOException, InterruptedException {
        return delegate.getSplits(context);
    }

    @Override
    public RecordReader<Text, SimpleFeature> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return delegate.createRecordReader(split, context);
    }

    @SuppressWarnings("unchecked")
    public static void configure(Job job, Map<String, String> dataStoreParams, Query query) {
        Object m = JavaConverters.mapAsScalaMapConverter(dataStoreParams).asScala();
        scala.collection.immutable.Map<String, String> scalaParams =
                ((scala.collection.mutable.Map<String, String>) m).toMap(Predef.<Tuple2<String, String>>conforms());
        GeoMesaAccumuloInputFormat$.MODULE$.configure(job, scalaParams, query);
    }

    @Deprecated
    @SuppressWarnings("unchecked")
    public static void configure(Job job,
                                 Map<String, String> dataStoreParams,
                                 String featureTypeName,
                                 String filter,
                                 String[] transform) {
        Object m = JavaConverters.mapAsScalaMapConverter(dataStoreParams).asScala();
        scala.collection.immutable.Map<String, String> scalaParams =
                ((scala.collection.mutable.Map<String, String>) m).toMap(Predef.<Tuple2<String, String>>conforms());
        Option<String> f = Option.apply(filter);
        Option<String[]> t = Option.apply(transform);
        GeoMesaAccumuloInputFormat$.MODULE$.configure(job, scalaParams, featureTypeName, f, t);
    }
}
