package org.locationtech.geomesa.tools.cassandra.commands

import java.io.{File, FileInputStream}
import java.util

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.geotools.data.Transaction
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.tools.cassandra.CassandraConnectionParams
import org.locationtech.geomesa.tools.cassandra.commands.CassandraIngestCommand.CassandraIngestParameters
import org.locationtech.geomesa.tools.common.{CLArgResolver, FeatureTypeSpecParam}
import org.locationtech.geomesa.tools.common.commands.Command



class CassandraIngestCommand(parent: JCommander)
  extends Command(parent)
    with CommandWithCassandraDataStore
    with LazyLogging {

  val command = "ingest"

  val params = new CassandraIngestParameters

  override def execute(): Unit = {



    val sft = CLArgResolver.getSft(params.spec)

    ds.createSchema(sft)

    val converterConfig = CLArgResolver.getConfig(params.config)

    val converter = SimpleFeatureConverters.build(sft, converterConfig)

    val file = new File(params.files.get(0)) //todo: handle multiple files

    val ec = converter.createEvaluationContext(Map("inputFilePath" -> file.getAbsolutePath))

    val is = new FileInputStream(file)

    val features = converter.process(is, ec)

    val fw = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)

    features.foreach {sf =>

      val toWrite = fw.next()

      toWrite.setAttributes(sf.getAttributes)
      toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
      toWrite.getUserData.putAll(sf.getUserData)
      toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)

      fw.write()

    }

    ds.dispose()

    IOUtils.closeQuietly(converter)
    IOUtils.closeQuietly(is)
    IOUtils.closeQuietly(fw)

  }


}

object CassandraIngestCommand {
  @Parameters(commandDescription = "Ingest into Cassandra")
  class CassandraIngestParameters extends CassandraConnectionParams
    with FeatureTypeSpecParam {


    @Parameter(names = Array("-C", "--converter"), description = "GeoMesa converter specification as a config string, file name, or name of an available converter")
    var config: String = null

    @Parameter(names = Array("-F", "--format"), description = "File format of input files (shp, csv, tsv, avro, etc)")
    var format: String = null

    @Parameter(names = Array("-t", "--threads"), description = "Number of threads if using local ingest")
    var threads: Integer = 1

    @Parameter(description = "<file>...", required = true)
    var files: java.util.List[String] = new util.ArrayList[String]()
  }


}