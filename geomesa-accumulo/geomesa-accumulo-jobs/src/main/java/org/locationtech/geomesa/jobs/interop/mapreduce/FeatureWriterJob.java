/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.interop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.geotools.data.Query;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.features.ScalaSimpleFeature;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes$;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Sample job showing how to read and write features using GeoMesa Input/Output formats
 *
 * This job assumes a feature has been created with the name 'myfeature' that contains a Geometry
 * attribute named 'geom' and a Date attribute named 'dtg'.
 *
 * The job copies each feature into a different simple feature type called 'test' and writes it
 * back to GeoMesa.
 */
public class FeatureWriterJob {

    public static class MyMapper extends Mapper<Text, SimpleFeature, Text, SimpleFeature> {

        static enum CountersEnum { FEATURES }

        Text text = new Text();
        SimpleFeatureType sft =
                SimpleFeatureTypes$.MODULE$.createType("test", "dtg:Date,*geom:Point:srid=4326");


        @Override
        public void map(Text key, SimpleFeature value, Context context)
                throws IOException, InterruptedException {
            Counter counter = context.getCounter(CountersEnum.class.getName(),
                                                 CountersEnum.FEATURES.toString());
            counter.increment(1);

            Object[] values = new Object[] { value.getAttribute("dtg"), value.getAttribute("geom") };
            SimpleFeature feature = new ScalaSimpleFeature(sft, value.getID(), values, null);
            context.write(text, feature);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "simple feature writer");

        job.setJarByClass(FeatureWriterJob.class);
        job.setMapperClass(MyMapper.class);
        job.setInputFormatClass(GeoMesaAccumuloInputFormat.class);
        job.setOutputFormatClass(GeoMesaOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ScalaSimpleFeature.class);
        job.setNumReduceTasks(0);

        Map<String, String> params = new HashMap<String, String>();
        params.put("instanceId", "myinstance");
        params.put("zookeepers", "zoo1,zoo2,zoo3");
        params.put("user", "myuser");
        params.put("password", "mypassword");
        params.put("tableName", "mycatalog");

        Query query = new Query("myfeature", ECQL.toFilter("BBOX(geom, -165,5,-50,75)"));

        GeoMesaAccumuloInputFormat.configure(job, params, query);

        Map<String, String> outParams = new HashMap<String, String>();
        outParams.put("instanceId", "myinstance");
        outParams.put("zookeepers", "zoo1,zoo2,zoo3");
        outParams.put("user", "myuser");
        outParams.put("password", "mypassword");
        outParams.put("tableName", "mycatalog_2");

        GeoMesaOutputFormat.configureDataStore(job, outParams);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
