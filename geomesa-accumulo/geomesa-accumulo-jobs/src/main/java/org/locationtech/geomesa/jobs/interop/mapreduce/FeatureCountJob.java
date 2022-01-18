/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.interop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.geotools.data.Query;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Sample job showing how to read features using GeoMesaInputFormat
 *
 * This job assumes a feature has been created with the name 'myfeature' that contains a Geometry
 * attribute named 'geom' and a Date attribute named 'dtg'.
 *
 * The job uses map/reduce counters to keep track of how many features are processed, and outputs
 * each feature ID and geometry to a text file in HDFS.
 */
public class FeatureCountJob {

    public static class MyMapper extends Mapper<Text, SimpleFeature, Text, Text> {

        static enum CountersEnum { FEATURES }

        @Override
        public void map(Text key, SimpleFeature value, Context context)
                throws IOException, InterruptedException {
            Counter counter = context.getCounter(CountersEnum.class.getName(),
                                                 CountersEnum.FEATURES.toString());
            counter.increment(1);
            context.write(key, new Text(value.getDefaultGeometry().toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "simple feature count");
        job.setJarByClass(FeatureCountJob.class);
        job.setMapperClass(MyMapper.class);
        job.setInputFormatClass(GeoMesaAccumuloInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        FileOutputFormat.setOutputPath(job, new Path("/tmp/myjob"));

        Map<String, String> params = new HashMap<>();
        params.put("instanceId", "myinstance");
        params.put("zookeepers", "zoo1,zoo2,zoo3");
        params.put("user", "myuser");
        params.put("password", "mypassword");
        params.put("tableName", "mycatalog");

        Query query = new Query("myfeature", ECQL.toFilter("BBOX(geom, -165,5,-50,75)"));

        GeoMesaAccumuloInputFormat.configure(job, params, query);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
