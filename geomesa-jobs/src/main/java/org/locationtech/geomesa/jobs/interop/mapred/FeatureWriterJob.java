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

package org.locationtech.geomesa.jobs.interop.mapred;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.geotools.data.Query;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.features.ScalaSimpleFeature;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
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

    public static class MyMapper implements Mapper<Text, SimpleFeature, Text, SimpleFeature> {

        static enum CountersEnum { FEATURES }

        Text text = new Text();
        SimpleFeatureType sft = SimpleFeatureTypes.createType("test2", "dtg:Date,*geom:Point:srid=4326");

        @Override
        public void map(Text key,
                        SimpleFeature value,
                        OutputCollector<Text, SimpleFeature> output,
                        Reporter reporter) throws IOException {
            Counter counter = reporter.getCounter(CountersEnum.class.getName(),
                                                  CountersEnum.FEATURES.toString());
            counter.increment(1);

            Object[] values = new Object[] { value.getAttribute("dtg"), value.getAttribute("geom") };
            SimpleFeature feature = new ScalaSimpleFeature(value.getID(), sft, values);
            output.collect(text, feature);
        }

        @Override
        public void close() throws IOException {}

        @Override
        public void configure(JobConf job) {}
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(FeatureCountJob.class);
        conf.setJobName("simple feature writing");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(ScalaSimpleFeature.class);

        conf.setMapperClass(MyMapper.class);
        conf.setNumReduceTasks(0);

        conf.setInputFormat(GeoMesaInputFormat.class);
        conf.setOutputFormat(GeoMesaOutputFormat.class);

        Map<String, String> params = new HashMap<String, String>();
        params.put("instanceId", "myinstance");
        params.put("zookeepers", "zoo1,zoo2,zoo3");
        params.put("user", "myuser");
        params.put("password", "mypassword");
        params.put("tableName", "mycatalog");

        Query query = new Query("myfeature", ECQL.toFilter("BBOX(geom, -165,5,-50,75)"));

        GeoMesaInputFormat.configure(conf, params, query);

        Map<String, String> outParams = new HashMap<String, String>();
        outParams.put("instanceId", "myinstance");
        outParams.put("zookeepers", "zoo1,zoo2,zoo3");
        outParams.put("user", "myuser");
        outParams.put("password", "mypassword");
        outParams.put("tableName", "mycatalog_2");

        GeoMesaOutputFormat.configureDataStore(conf, outParams);

        JobClient.runJob(conf);
    }
}
