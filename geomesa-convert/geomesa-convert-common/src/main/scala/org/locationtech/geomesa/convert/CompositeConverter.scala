package org.locationtech.geomesa.convert

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Predicate}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class CompositeConverterFactory[I] extends SimpleFeatureConverterFactory[I] {
  override def canProcess(conf: Config): Boolean = canProcessType(conf, "composite-converter")

  override def buildConverter(sft: SimpleFeatureType, conf: Config): SimpleFeatureConverter[I] = {
    val converters: Seq[(Predicate, SimpleFeatureConverter[I])] =
      conf.getConfigList("converters").map { c =>
        val pred = Transformers.parsePred(c.getString("predicate"))
        val converter = SimpleFeatureConverters.build[I](sft, conf.getConfig(c.getString("converter")))
        (pred, converter)
      }
    new CompositeConverter[I](sft, converters)
  }

}

class CompositeConverter[I](val targetSFT: SimpleFeatureType,
                               converters: Seq[(Predicate, SimpleFeatureConverter[I])])
  extends SimpleFeatureConverter[I] {

  // to maintain laziness
  val convView = converters.view

  override def processInput(is: Iterator[I]): Iterator[SimpleFeature] =
    is.map { input =>
      convView.flatMap { case (pred, conv) =>  processIfValid(input, pred, conv) }.head
    }

  // noop
  override def processSingleInput(i: I): SimpleFeature = null

  implicit val ec = new EvaluationContext(Map(), Array())
  def processIfValid(input: I, pred: Predicate, conv: SimpleFeatureConverter[I]) =
    if(pred.eval(input)) Some(conv.processSingleInput(input)) else None
}



