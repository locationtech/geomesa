/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fb054a34dc (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> fb054a34dc (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fb054a34dc (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.jobs.mapreduce.interop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Input format that will read simple features from GeoMesa based on a CQL query.
 * The key will be the feature ID. Configure using the static methods.
 */
public class GeoMesaAccumuloInputFormat extends InputFormat<Text, SimpleFeature> {

    private org.locationtech.geomesa.accumulo.jobs.mapreduce.GeoMesaAccumuloInputFormat delegate =
            new org.locationtech.geomesa.accumulo.jobs.mapreduce.GeoMesaAccumuloInputFormat();

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

    public static void configure(Job job, Map<String, String> dataStoreParams, Query query) {
        org.locationtech.geomesa.accumulo.jobs.mapreduce.GeoMesaAccumuloInputFormat
              .configure(job.getConfiguration(), dataStoreParams, query);
    }
}
