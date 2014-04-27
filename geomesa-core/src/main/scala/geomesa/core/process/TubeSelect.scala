package geomesa.process

import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.visitor.{AbstractCalcResult, CalcResult, FeatureCalc}
import org.geotools.process.factory.{DescribeResult, DescribeParameter, DescribeProcess}
import org.geotools.process.vector.VectorProcess
import org.geotools.util.NullProgressListener
import org.opengis.feature.Feature
import java.util.Date

@DescribeProcess(
  title = "Performs a tube select on one feature based on another",
  description = "Returns a feature collection"
)
class TubeSelect extends VectorProcess {

  @DescribeResult(description = "Output feature collection")
  def  execute(
                @DescribeParameter(
                  name = "lineString",
                  description = "The line string around which to select features")
                lineString: Geometry,

                @DescribeParameter(
                  name = "featureCollection",
                  description = "The data set to query")
                collection: SimpleFeatureCollection,

                @DescribeParameter(
                  name = "startDate",
                  description = "The start date of the query")
                startDate: Date,

                @DescribeParameter(
                  name = "endDate",
                  description = "The start date of the query")
                endDate: Date

                ): SimpleFeatureCollection = {

    // assume for now that firstFeatures is a singleton collection
    val tubeVisitor = new TubeVisitor(lineString, startDate, endDate)
    collection.accepts(tubeVisitor, new NullProgressListener)
    tubeVisitor.getResult.asInstanceOf[TubeResult].results
  }

}

class TubeVisitor(val geom: Geometry, val startDate: Date, val endDate: Date) extends FeatureCalc {

  var resultCalc: TubeResult = null

  def visit(feature: Feature): Unit = {}

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) {
    resultCalc = TubeResult(r)
  }
}

case class TubeResult(results: SimpleFeatureCollection) extends AbstractCalcResult