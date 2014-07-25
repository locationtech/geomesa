package geomesa.core.process.knn

import org.geotools.referencing.crs.DefaultGeographicCRS


import collection.JavaConverters._

import geomesa.core.index
import geomesa.core.data._
import geomesa.core.index.Constants
import geomesa.utils.geohash.GeoHash
import org.geotools.data.{Query, DataStoreFinder, DataUtilities}
import org.geotools.referencing.CRS
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.geotools.factory.CommonFactoryFinder

@RunWith(classOf[JUnitRunner])
class GenerateKNNQueryTest extends Specification {

  sequential

   def createStore: AccumuloDataStore =
   // the specific parameter values should not matter, as we
   // are requesting a mock data store connection to Accumulo
     DataStoreFinder.getDataStore(Map(
       "instanceId" -> "mycloud",
       "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
       "user"       -> "myuser",
       "password"   -> "mypassword",
       "auths"      -> "A,B,C",
       "tableName"  -> "testwrite",
       "useMock"    -> "true",
       "featureEncoding" -> "avro").asJava).asInstanceOf[AccumuloDataStore]

  val sftName = "test"
  val sft = DataUtilities.createType(sftName, index.spec)
  sft.getUserData.put(Constants.SF_PROPERTY_START_TIME,"dtg")

  val ds = createStore

  ds.createSchema(sft)
  val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

  val smallGH =  GeoHash("dqb0tg")

  val ff          = CommonFactoryFinder.getFilterFactory2
  val WGS84       = DefaultGeographicCRS.WGS84


    "GenerateKNNQuery" should {
      "inject a small BBOX into a larger query and have the spatial predicate be equal to the GeoHash boundary" in {
        // setup a spatial filter, taken from FilterToAccumuloTest
        val q =
        ff.and(
          ff.like(ff.property("prop"), "foo"),
          ff.bbox("geom", -80.0, 30, -70, 40, CRS.toSRS(WGS84))
        )
        val f2a = new FilterToAccumulo(sft)


        // use the above to generate a Query
        val oldQuery = new Query(sftName, q)

        // and then generate a new one
        val newQuery = KNNQuery.generateKNNQuery(smallGH, oldQuery, fs)

        // check that oldQuery is untouched
        val oldFilter = oldQuery.getFilter

        // process the newQuery
        val newFilter =  f2a.visit(newQuery)

        // get the extracted spatial predicate
        val newPolygon = f2a.spatialPredicate


        // confirm that the oldFilter was not mutated by operations on the new filter
        // this confirms that the deep copy on the oldQuery was done properly
        oldFilter mustEqual q

        // confirm that the extracted spatial predicate matches the GeoHash BBOX.
        newPolygon.equalsExact(smallGH.geom) must beTrue

        //confirm that the newFilter has all spatial predicates removed
        newFilter mustEqual ff.like(ff.property("prop"), "foo")


      }

    }

 }
