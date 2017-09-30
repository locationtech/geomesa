package org.locationtech.geomesa.convert.simplefeature

import com.typesafe.config.Config
import org.locationtech.geomesa.convert._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.immutable

class SimpleFeatureConverterFactory extends AbstractSimpleFeatureConverterFactory[SimpleFeature] {

  override protected def typeToProcess: String = "simple-feature"

  override protected def buildConverter(sft: SimpleFeatureType,
                                        conf: Config,
                                        idBuilder: Transformers.Expr,
                                        fields: immutable.IndexedSeq[Field],
                                        userDataBuilder: Map[String, Transformers.Expr],
                                        cacheServices: Map[String, EnrichmentCache],
                                        parseOpts: ConvertParseOpts): SimpleFeatureConverter[SimpleFeature] =
    new SimpleFeatureSimpleFeatureConverter(sft, idBuilder, fields, userDataBuilder, cacheServices, parseOpts)

  override def buildFields(conf: Config): IndexedSeq[Field] = {
    import scala.collection.JavaConversions._
    conf.getConfigList("fields").map(buildField).toIndexedSeq
  }

  override def buildField(field: Config): Field =
    SimpleField(field.getString("name"), Transformers.parseTransform(field.getString("transform")))


}

case class SimpleFeatureField(attr: String) extends Field {

  override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = {
    ec.
  }
}
