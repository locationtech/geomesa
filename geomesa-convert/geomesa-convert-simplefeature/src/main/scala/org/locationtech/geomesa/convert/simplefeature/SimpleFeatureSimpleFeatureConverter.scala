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


  override def fromInputType(i: SimpleFeature): Seq[Array[Any]] = Seq(i.getAttributes.toArray.asInstanceOf[Array[Any]] ++ Array(i.getID))

  override def process(is: InputStream, ec: EvaluationContext): Iterator[SimpleFeature] = ???
}
