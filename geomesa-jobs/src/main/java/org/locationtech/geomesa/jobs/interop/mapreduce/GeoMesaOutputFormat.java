/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.interop.mapreduce;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat$;
import org.opengis.feature.simple.SimpleFeature;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.Map;

/**
 * Output format for writing simple features to GeoMesa. The key will be ignored. SimpleFeatureTypes
 * will be created in GeoMesa as needed based on the simple features passed.
 *
 * Configure using the static methods.
 */
public class GeoMesaOutputFormat extends OutputFormat<Text, SimpleFeature> {

    private org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat delegate =
            new org.locationtech.geomesa.jobs.mapreduce.GeoMesaOutputFormat();

    @Override
    public RecordWriter<Text, SimpleFeature> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return delegate.getRecordWriter(context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        delegate.checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return delegate.getOutputCommitter(context);
    }

    public static void configureDataStore(Job job, Map<String, String> dataStoreParams) {
        scala.collection.immutable.Map<String, String> scalaParams =
                JavaConverters.asScalaMapConverter(dataStoreParams).asScala()
                              .toMap(Predef.<Tuple2<String, String>>conforms());
        GeoMesaOutputFormat$.MODULE$.configureDataStore(job, scalaParams);
    }

    public static void configureBatchWriter(Job job, BatchWriterConfig writerConfig) {
        GeoMesaOutputFormat$.MODULE$.configureBatchWriter(job, writerConfig);
    }
}
