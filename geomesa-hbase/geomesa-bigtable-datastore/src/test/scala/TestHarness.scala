/**
 * Created by afox on 6/10/15.
 */
class TestHarness {

  import org.geotools.data._
  import org.geotools.factory.CommonFactoryFinder
  import org.joda.time.DateTime

  import scala.collection.JavaConversions._


  val ds = DataStoreFinder.getDataStore(Map("bigtable.table.name" -> "descartes"))
  val fs = ds.getFeatureSource("staging").asInstanceOf[org.locationtech.geomesa.hbase.data.HBaseFeatureSource]


/*
  val w = fs.getWriter(null.asInstanceOf[Query])
  val f = w.next
  f.getIdentifier.asInstanceOf[FeatureIdImpl].setID("2")
  f.setAttribute("name", "hello")
  f.setAttribute("dtg", new java.util.Date())
  val gf = JTSFactoryFinder.getGeometryFactory
  val pt = gf.createPoint(new Coordinate(35.5,36.6))
  f.setDefaultGeometry(pt)
  f.getAttributes
  w.write
*/

  val ff = CommonFactoryFinder.getFilterFactory2
  val filt = ff.and(ff.bbox("geom", -120, 30, -70, 65, "EPSG:4326"),
    ff.between(ff.property("dtg"),
      ff.literal(new DateTime("2014-05-09T23:00:00.000Z").toDate),
      ff.literal(new DateTime("2014-05-11T23:00:00.000Z").toDate)))

  val res = fs.getFeatures(filt).features()
}
