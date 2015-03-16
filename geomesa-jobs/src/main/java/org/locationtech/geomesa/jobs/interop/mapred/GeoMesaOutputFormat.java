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

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.locationtech.geomesa.jobs.mapred.GeoMesaOutputFormat$;
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
public class GeoMesaOutputFormat implements OutputFormat<Text, SimpleFeature> {

    private org.locationtech.geomesa.jobs.mapred.GeoMesaOutputFormat delegate =
            new org.locationtech.geomesa.jobs.mapred.GeoMesaOutputFormat();

    @Override
    public RecordWriter<Text, SimpleFeature> getRecordWriter(FileSystem ignored,
                                                             JobConf job,
                                                             String name,
                                                             Progressable progress) throws IOException {
        return delegate.getRecordWriter(ignored, job, name, progress);
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        delegate.checkOutputSpecs(ignored, job);
    }

    public static void configureDataStore(JobConf job, Map<String, String> dataStoreParams) {
        scala.collection.immutable.Map<String, String> scalaParams =
                JavaConverters.asScalaMapConverter(dataStoreParams).asScala()
                              .toMap(Predef.<Tuple2<String, String>>conforms());
        GeoMesaOutputFormat$.MODULE$.configureDataStore(job, scalaParams);
    }

    public static void configureBatchWriter(JobConf job, BatchWriterConfig writerConfig) {
        GeoMesaOutputFormat$.MODULE$.configureBatchWriter(job, writerConfig);
    }
}
