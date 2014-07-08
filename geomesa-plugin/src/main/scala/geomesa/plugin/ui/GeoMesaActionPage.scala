/*
  * Copyright 2014 Commonwealth Computer Research, Inc.
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

package geomesa.plugin.ui

import java.io.File

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.wicket.ajax.AjaxRequestTarget
import org.apache.wicket.ajax.markup.html.AjaxLink

class GeoMesaActionPage extends GeoMesaBasePage {

  add(new AjaxLink("ingest") {
    protected override def onClick(target: AjaxRequestTarget) {
      val conf = GeoMesaBasePage.getHdfsConfiguration

      val fs = FileSystem.get(conf)

      // TODO make this run a useful m/r job - calculate stats or something instead of word-count

      val tempFile = File.createTempFile("wordcount", ".jar")
      tempFile.delete()

      // TODO make job configurable - don't use hard-coded values here
      fs.copyToLocalFile(new Path("/path/on/hdfs/wordcount-0.1.0-SNAPSHOT.jar"),
                          new Path(tempFile.getAbsolutePath))

      conf.set("mapreduce.map.class", "com.ccri.WordCountMap")
      conf.set("mapreduce.combine.class", "com.ccri.WordCountReduce")
      conf.set("mapreduce.reduce.class", "com.ccri.WordCountReduce")

      ToolRunner.run(conf, new RemoteTool, Array(
        classOf[RemoteTool].getCanonicalName,
        "--geomesa.temp.jar", tempFile.getAbsolutePath))

      println("done")

    }
  })

}

class RemoteTool extends Configured with Tool {

  override def run(args: Array[String]): Int = {

    val jarPath = {
      args(args.indexOf("--geomesa.temp.jar") + 1)
    }

    println("tool path: " + jarPath)

    val conf = getConf

    val fs = FileSystem.get(conf)

    val outputDir = new Path("/path/on/hdfs/output/")
    if (fs.exists(outputDir)) {
      // remove this directory, if it already exists
      fs.delete(outputDir, true)
    }

    val job = Job.getInstance(conf, "wordcount")

    job.setJar(jarPath)

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    FileOutputFormat.setOutputPath(job, outputDir)

    FileInputFormat.setInputPaths(job, new Path("/path/on/hdfs/input/"))

    val result = job.waitForCompletion(true)

    println(s"done with job: $result")

    if (result) 0 else 1
  }
}