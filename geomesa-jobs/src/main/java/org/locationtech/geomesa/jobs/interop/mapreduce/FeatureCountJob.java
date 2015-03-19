/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.jobs.interop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Sample job showing how to read features using GeoMesaInputFormat
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
            context.write(key, new Text(value.getAttribute("geom").toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "simple feature count");
        job.setJarByClass(FeatureCountJob.class);
        job.setMapperClass(MyMapper.class);
        job.setInputFormatClass(GeoMesaInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        FileOutputFormat.setOutputPath(job, new Path("/tmp/myjob"));

        Map<String, String> params = new HashMap<String, String>();
        params.put("instanceId", "myinstance");
        params.put("zookeepers", "zoo1,zoo2,zoo3");
        params.put("user", "myuser");
        params.put("password", "mypassword");
        params.put("tableName", "mycatalog");

        String cql = "BBOX(geom, -165,5,-50,75) AND dtg DURING 2015-03-02T00:00:00.000Z/2015-03-02T23:59:59.999Z";

        GeoMesaInputFormat.configure(job, params, "myfeature", cql);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
