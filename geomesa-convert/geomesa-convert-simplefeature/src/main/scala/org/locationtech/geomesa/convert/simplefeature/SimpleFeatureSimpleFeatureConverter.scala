package org.locationtech.geomesa.convert.simplefeature

import java.io.InputStream

import org.locationtech.geomesa.convert._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class SimpleFeatureSimpleFeatureConverter(inputSFT: SimpleFeatureType,
                                          val targetSFT: SimpleFeatureType,
                                          val idBuilder: Transformers.Expr,
                                          val inputFields: IndexedSeq[Field],
                                          val userDataBuilder: Map[String, Transformers.Expr],
                                          val caches: Map[String, EnrichmentCache],
                                          val parseOpts: ConvertParseOpts) extends ToSimpleFeatureConverter[SimpleFeature] {


  /**
    * Process a single input (e.g. line)
    */
  override def processSingleInput(i: SimpleFeature, ec: EvaluationContext): Seq[SimpleFeature] = {
    import scala.collection.JavaConversions._
    ec.clear()
    ec.counter.incLineCount()

    val in = i.getAttributes ++ Array(i.getID)
    val res = convert(in.toArray, ec)
    if(res == null) ec.counter.incFailure()
    else ec.counter.incSuccess()
    Seq(res)
  }


  override def fromInputType(i: SimpleFeature): Seq[Array[Any]] = ???

  override def process(is: InputStream, ec: EvaluationContext): Iterator[SimpleFeature] = ???
}
