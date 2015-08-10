/***********************************************************************
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *************************************************************************/

package org.locationtech.geomesa.jobs.interop.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.*;
import org.geotools.data.Query;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Sample job showing how to read features using GeoMesaInputFormat.
 *
 * This job assumes a feature has been created with the name 'myfeature' that contains a Geometry
 * attribute named 'geom' and a Date attribute named 'dtg'.
 *
 * The job uses map/reduce counters to keep track of how many features are processed, and outputs
 * each feature ID and geometry to a text file in HDFS.
 */
public class FeatureCountJob {

    public static class MyMapper extends MapReduceBase implements Mapper<Text, SimpleFeature, Text, Text> {

        static enum CountersEnum { FEATURES }

        public void map(Text key,
                        SimpleFeature value,
                        OutputCollector<Text, Text> output,
                        Reporter reporter) throws IOException {
            Counter counter = reporter.getCounter(CountersEnum.class.getName(),
                                                  CountersEnum.FEATURES.toString());
            counter.increment(1);
            output.collect(new Text(value.getID()), new Text(value.getDefaultGeometry().toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(FeatureCountJob.class);
        conf.setJobName("simple feature count");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MyMapper.class);
        conf.setNumReduceTasks(0);

        conf.setInputFormat(GeoMesaInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(conf, new Path("/tmp/myjob"));

        Map<String, String> params = new HashMap<>();
        params.put("instanceId", "myinstance");
        params.put("zookeepers", "zoo1,zoo2,zoo3");
        params.put("user", "myuser");
        params.put("password", "mypassword");
        params.put("tableName", "mycatalog");

        Query query = new Query("myfeature", ECQL.toFilter("BBOX(geom, -165,5,-50,75)"));

        GeoMesaInputFormat.configure(conf, params, query);

        JobClient.runJob(conf);
    }
}
