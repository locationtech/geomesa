package org.locationtech.geomesa.convert.xml

import javax.xml.parsers.SAXParserFactory

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Expr}
import org.locationtech.geomesa.convert._
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.xml.Elem

class XMLConverter(val targetSFT: SimpleFeatureType,
                   val idBuilder: Expr,
                   val inputFields: IndexedSeq[Field]) extends ToSimpleFeatureConverter[String] {

  private val parser = SAXParserFactory.newInstance().newSAXParser()
  private val loader = scala.xml.XML.withSAXParser(parser)

  override def fromInputType(i: String): Array[Any] = {
    parser.reset()
    Array(loader.loadString(i))
  }

}

class XMLConverterFactory extends SimpleFeatureConverterFactory[String] {
  override def canProcess(conf: Config): Boolean = canProcessType(conf, "xml")

  override def buildConverter(sft: SimpleFeatureType, conf: Config): XMLConverter = {
    val fields    = buildFields(conf.getConfigList("fields"))
    val idBuilder = buildIdBuilder(conf.getString("id-field"))
    new XMLConverter(sft, idBuilder, fields)
  }

  override def buildFields(fields: Seq[Config]): IndexedSeq[Field] = {
    fields.map { f =>
      val name = f.getString("name")
      val transform =
        if (f.hasPath("transform"))
          Transformers.parseTransform(f.getString("transform"))
        else
          null
      if (f.hasPath("path"))
        XMLField(name, f.getStringList("path").toList, transform)
      else
        SimpleField(name, transform)
    }.toIndexedSeq
  }
}

case class XMLField(name: String, xmlPath: List[String], transform: Expr) extends Field {

  private val mutableArray = Array.ofDim[Any](1)
  private val root::leafs = xmlPath

  override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = {
    val elem = args(0).asInstanceOf[Elem]
    mutableArray(0) = leafs.foldLeft(elem \ root)(_ \ _).text
    if (transform == null)
      mutableArray(0)
    else
      super.eval(mutableArray)
  }

}
