package org.locationtech.geomesa.convert.avro

import com.typesafe.config.Config
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.locationtech.geomesa.convert.Transformers.Expr
import org.locationtech.geomesa.convert.{Field, SimpleFeatureConverter, SimpleFeatureConverterFactory, ToSimpleFeatureConverter}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class Avro2SimpleFeatureConverterBuilder extends SimpleFeatureConverterFactory[Array[Byte]] {

  override def canProcess(conf: Config): Boolean = canProcessType(conf, "avro")

  override def buildConverter(targetSFT: SimpleFeatureType, conf: Config): SimpleFeatureConverter[Array[Byte]] = {
    val avroSchemaPath = conf.getString("schema")
    val avroSchema = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream(avroSchemaPath))
    val reader = new GenericDatumReader[GenericRecord](avroSchema)
    val fields = buildFields(conf.getConfigList("fields"))
    val idBuilder = buildIdBuilder(conf.getString("id-field"))

    new Avro2SimpleFeatureConverter(avroSchema, reader, targetSFT, fields, idBuilder)
  }

}

class Avro2SimpleFeatureConverter(avroSchema: Schema,
                                  reader: GenericDatumReader[GenericRecord],
                                  val targetSFT: SimpleFeatureType,
                                  val inputFields: IndexedSeq[Field],
                                  val idBuilder: Expr)
  extends ToSimpleFeatureConverter[Array[Byte]] {

  var decoder: BinaryDecoder = null
  var recordReuse: GenericRecord = null

  override def fromInputType(bytes: Array[Byte]): Array[Any] = {
    decoder = DecoderFactory.get.binaryDecoder(bytes, decoder)
    Array(bytes, reader.read(recordReuse, decoder))
  }

}
