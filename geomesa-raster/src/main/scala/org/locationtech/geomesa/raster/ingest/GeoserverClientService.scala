/**
 * Copyright (c) Commonwealth Computer Research, Inc. 2006 - 2012
 * All Rights Reserved, www.ccri.com
 *
 * Developed under contracts for:
 * US Army / RDECOM / CERDEC / I2WD and
 * SBIR Contract for US Navy / Office of Naval Research
 *
 * This code may contain SBIR protected information.  Contact CCRi prior to
 * distribution.
 *
 * Begin SBIR Data Rights Statement
 * Contract No.:  N00014-08-C-0254
 * Contractor Name:  Commonwealth Computer Research, Inc
 * Contractor Address:  1422 Sachem Pl, Unit #1, Charlottesville, VA 22901
 * Expiration of SBIR Data Rights Period:  5/9/2017
 * The Government's rights to use, modify, reproduce, release, perform, display,
 * or disclose technical data or computer software marked with this legend are
 * restricted during the period shown as provided in paragraph (b)(4) of the
 * Rights in Noncommercial Technical Data and Computer Software--Small Business
 * Innovative Research (SBIR) Program clause contained in the above identified
 * contract. No restrictions apply after the expiration date shown above. Any
 * reproduction of technical data, computer software, or portions thereof
 * marked with this legend must also reproduce the markings.
 * End SBIR Data Rights Statement
 **/

package org.locationtech.geomesa.raster.ingest

import akka.actor.{Props, Actor}
import akka.pattern.ask
import akka.util.Timeout
import akka.actor.Status.Failure
import scala.concurrent.duration.{Duration, FiniteDuration}
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import org.apache.http.entity.{StringEntity, ContentType}
import org.apache.http.client.methods._
import org.apache.http.impl.client.{BasicCredentialsProvider, DefaultHttpClient}
import org.apache.http.auth.{UsernamePasswordCredentials, AuthScope}
import org.apache.http.util.EntityUtils
import org.apache.http.message.BasicHeader
import scala.collection.mutable
import scala.xml.Elem
import com.typesafe.scalalogging.slf4j.Logging

object GeoserverClientService extends Logging {
  val credentials = new UsernamePasswordCredentials(config.geoserverUsername,
    config.geoserverPassword)

  val restURL = s"${config.geoserverUrl}/rest"
  val namespaceUrl = s"${config.geoserverNamespaceUrl}"
  val layersUrl = s"$restURL/layers"
  val dataStoresURL = s"$restURL/workspaces/${config.layersNamespace}/datastores"
  val coverageStoresURL = s"$restURL/workspaces/${config.layersNamespace}/coveragestores"
  val geoWebCacheURL = s"${config.geoserverUrl}/gwc/rest/layers"
  val gwcStyles = config.gwcDefaultStyles.split(",").
        map(suffix => config.stylesNamespace + suffix).toSeq
  val stylesUrl = s"$restURL/workspaces/${config.layersNamespace}/styles"
  val globalStylesUrl = s"$restURL/styles"
  val spinozaDefaultName = "spinoza_all"
  val geometryTypeName = "the_geom"
  val coverageFormatName = "Accumulo Coverage Format"

  lazy val actorSystem = ActorSystemGenerator.getActorSystem("gc", "")
  lazy val registrationActor = actorSystem.actorOf(Props(new RegistrationActor))

  def registrationData(surfaceId: String, styleName: String, connectConfig: Map[String, Option[String]]):
    RegistrationData = {
    val url = coverageURL(surfaceId, connectConfig)
    val coverageName = surfaceId
    val storeName = connectConfig(IngestRasterParams.TABLE).get + ":" + coverageName
    RegistrationData(url, storeName, coverageName, styleName)
  }

  def coverageURL(surfaceId: String, params: Map[String, Option[String]]): String = {
    val zookeepers = params(IngestRasterParams.ZOOKEEPERS).get
    val instance = params(IngestRasterParams.ACCUMULO_INSTANCE).get
    val tableName = params(IngestRasterParams.TABLE).get
    val user = params(IngestRasterParams.ACCUMULO_USER).get
    val password = params(IngestRasterParams.ACCUMULO_PASSWORD).get
    val auths = params(IngestRasterParams.AUTHORIZATIONS).get

    s"accumulo://$user:$password@$instance/" +
      s"$tableName/$surfaceId/#zookeepers=$zookeepers#auths=$auths"
  }

  def registerSurface(surfaceId: String,
                      styleName: Option[String],
                      connectConfig: Map[String, Option[String]]) {
    implicit val timeout = Timeout(config.geoserverRegistrationTimeout)
    Await.ready(registrationActor ? registrationData(surfaceId,
      styleName.getOrElse(config.continuousThreatStyleName), connectConfig), timeout.duration)
  }

  /**
   *  A small abstraction to support both isCoverageRegistered and isGWCRegistered
   * @param url             Url to test.
   * @param successMessage  Option(al) success log message
   * @param failureMessage  Option(al) failure log message
   * @return
   */
  def doesGetReturn(url: String, successMessage: String, failureMessage: String): Boolean =
    try {
      get(url)
      logger.debug(successMessage)
      true
    } catch {
      case _: Throwable => {
        logger.debug(failureMessage)
        false
      }
    }

  def registerWorkspace(name: String) {
    val xml =
      <workspace>
        <name>{ name }</name>
      </workspace>.toString()
    post(s"$restURL/workspaces", ContentType.TEXT_XML, xml)
  }

  def registerWorkspaceIfNotRegistered() {
    if (!isWorkspaceRegistered(config.layersNamespace)) registerWorkspace(config.layersNamespace)
  }

  def registerFeatureType(id: String, name: String, dataStoreName: String)  {
    logger.debug(s"Attempting to publish $name")
    val featureTypesUrl = s"$dataStoresURL/$dataStoreName/featuretypes/"
    if (isFeatureTypeRegistered(name, dataStoreName)) {
      logger.debug(s"Removing existing featureType and layer:  name $name; data store name $dataStoreName")
      delete(s"$layersUrl/${config.layersNamespace}:$name")
      delete(s"$dataStoresURL/$dataStoreName/featuretypes/$name")
    }
    val latLonBoundingBox = GeoserverBoundingBox.WHOLE_EARTH
    val xml =
      <featureType>
        <name>{ name }</name>
        <nativeName>{ id }</nativeName>
        <title>{ name }</title>
        <srs>EPSG:4326</srs>
        <nativeBoundingBox>
          <minx>{ latLonBoundingBox.minx }</minx>
          <maxx>{ latLonBoundingBox.maxx }</maxx>
          <miny>{ latLonBoundingBox.miny }</miny>
          <maxy>{ latLonBoundingBox.maxy }</maxy>
          <crs>{ latLonBoundingBox.crs }</crs>
        </nativeBoundingBox>
        <latLonBoundingBox>
          <minx>{ latLonBoundingBox.minx }</minx>
          <maxx>{ latLonBoundingBox.maxx }</maxx>
          <miny>{ latLonBoundingBox.miny }</miny>
          <maxy>{ latLonBoundingBox.maxy }</maxy>
          <crs>{ latLonBoundingBox.crs }</crs>
        </latLonBoundingBox>
        <projectionPolicy>FORCE_DECLARED</projectionPolicy>
      </featureType>.toString()
    logger.debug(s"POSTING: $xml")
    post(featureTypesUrl, ContentType.TEXT_XML, xml)

    //Below codes explicitly set default style for the feature type to be registered. It is due to
    //current geomesa plugin on geoserver doesn't set correct default style for a feature layer.
    //It may not needed when geomesa plugin is upgraded on geoserver in future.
    modifyDefaultStyle(name, spinozaDefaultName)
  }

  def isWorkspaceRegistered(name: String): Boolean =
    doesGetReturn(s"$restURL/workspaces/$name",
      s"Workspace is registered: $name",
      s"Workspace is NOT registered: $name")

  def isFeatureTypeRegistered(name: String, dataStoreName: String): Boolean =
    doesGetReturn(s"$dataStoresURL/$dataStoreName/featuretypes/$name",
      s"FeatureType is registered: $name",
      s"FeatureType is NOT registered: $name")

  def isDataStoreRegistered(name: String): Boolean =
    doesGetReturn(s"$dataStoresURL/$name",
      s"DataStore is registered:  $name",
      s"DataStore is NOT registered:  $name")

  def isCoverageRegistered(name: String): Boolean =
    doesGetReturn(s"$coverageStoresURL/$name",
      s"Coverage is registered:  $name",
      s"Coverage is NOT registered:  $name")

  def isGWCRegistered(name: String): Boolean =
    doesGetReturn(s"$geoWebCacheURL/${config.layersNamespace}:$name.xml",
      s"Layer is already registered with GWC:  $name",
      s"Layer is NOT registered with GWC:  $name")

  def isStyleRegistered(name: String): Boolean =
    doesGetReturn(s"$stylesUrl/$name",
      s"Style is registered:  $name",
      s"Style is NOT registered:  $name")

  def isGlobalStyleRegistered(name: String): Boolean =
    doesGetReturn(s"$globalStylesUrl/$name",
      s"Global Style is registered:  $name",
      s"Global Style is NOT registered:  $name")

  def registerDataStore(name: String, dataStoreType: String, connectionParameters: Map[String, String]): Int = {
    val updatedConnectionParameters = connectionParameters + ("namespace" -> namespaceUrl)

    if (isDataStoreRegistered(name)) {
      logger.debug(s"Updating existing data store:  name $name; DataStore type $dataStoreType; Datastore URL: $dataStoresURL")
      put(s"$dataStoresURL/$name",
        ContentType.TEXT_XML,
        dataStoreBody(name, dataStoreType, true, config.layersNamespace, updatedConnectionParameters))
    }
    else {
      logger.debug(s"Registering new data store:  name $name; DataStore type $dataStoreType; Datastore URL: $dataStoresURL")
      post(dataStoresURL,
        ContentType.TEXT_XML,
        dataStoreBody(name, dataStoreType, true, config.layersNamespace, updatedConnectionParameters))
    }
  }

  def dataStoreBody(name: String,
                    formatName: String,
                    enabled: Boolean,
                    workspace: String,
                    connectionParameters: Map[String, String]) = {
    <dataStore>
      <name>{ name }</name>
      <type>{ formatName }</type>
      <enabled>{ enabled }</enabled>
      <workspace>
        <id>{ workspace }</id>
      </workspace>
      <connectionParameters>
        { for (param <- connectionParameters) yield <entry key={ param._1 }>{ param._2 }</entry> }
      </connectionParameters>
    </dataStore>.toString()
  }

  def registerEventStyle(name: String, iconFieldName: Option[String], rules: Map[String, String], defaultIconURL: String) = {
    val method:HttpEntityEnclosingRequestBase = if (isStyleRegistered(name)) new HttpPut(s"$stylesUrl/$name") else new HttpPost(stylesUrl)

    val colors = List[String]("FFFF99", "FFFF00", "FFCC99", "FFCC00", "FF99CC", "FF9900", "FF6600", "FF00FF", "FF0000",
      "CCFFFF", "CCFFCC", "CC99FF", "C0C0C0", "99CCFF", "99CC00", "993366", "993300", "969696", "808080", "808000",
      "800080", "800000", "666699", "33CCCC", "339966", "3366FF", "333399", "333333", "333300", "00FFFF", "00FF00",
      "00CCFF", "008080", "008000", "0033CC", "003300", "0000FF", "000080", "000000")

    val entity = new StringEntity(eventSldBody(name, iconFieldName, rules, defaultIconURL, colors), ContentType.TEXT_XML)
    method.setEntity(entity)
    method.setHeader(new BasicHeader("Content-type", "application/vnd.ogc.sld+xml"))
    execute(method)
    modifyDefaultStyleNonGWC(name, name)
  }

  def eventSldBody(name: String, iconFieldName: Option[String], rules: Map[String, String], defaultIconURL: String, iconColors: List[String]) = {
    require(defaultIconURL.contains("[[COLOR]]"), "default event icon URL must contain [[COLOR]] placeholder")

    val condensedRuleMap = new mutable.HashMap[String, mutable.ListBuffer[String]]()
    for ((k: String, v: String) <- rules) yield {
      if (condensedRuleMap.contains(v)) {
        condensedRuleMap.put(v, condensedRuleMap.get(v).get += k)
      } else {
        condensedRuleMap.put(v, new mutable.ListBuffer[String]() += k)
      }
    }

    val condensedSLDRules: mutable.HashMap[String, Elem] = for ((k: String, vs: mutable.ListBuffer[String]) <- condensedRuleMap) yield {
      vs.size match {
        case 1 => {
          (k, <ogc:PropertyIsEqualTo>
            <ogc:PropertyName>{ iconFieldName.get }</ogc:PropertyName>
            <ogc:Literal>{ vs(0) }</ogc:Literal>
          </ogc:PropertyIsEqualTo>)
        }
        case n if n < 11 && n > 1 => {
          (k, <ogc:PropertyIsEqualTo>
            <ogc:Function name={"in" + n}>
              <ogc:PropertyName>{ iconFieldName.get }</ogc:PropertyName>
              { for (v <- vs) yield
              <ogc:Literal>{ v }</ogc:Literal>
              }
            </ogc:Function>
            <ogc:Literal>true</ogc:Literal>
          </ogc:PropertyIsEqualTo>)
        }
        case _ => throw new Exception("cannot support iconMapping of over 10 unique field values to the same icon") //without very ugly nesting
      }
    }

    def nestOr(toNest: mutable.HashMap[String, Elem]): Elem = {
      if (toNest.size == 1) { toNest.head._2 }
      else {
        <ogc:Or>
          { toNest.head._2 }
          { nestOr(toNest.drop(1)) }
        </ogc:Or>
      }
    }

    val elseFilter = condensedSLDRules.size match {
      case 0 => { None }
      case _ => { Some(<ogc:Not>{ nestOr(condensedSLDRules) }</ogc:Not>) }
    }

    def colorWithElseFilter(color: String): Elem = {
      def colorFilter(color: String): Elem = {
        <ogc:PropertyIsEqualTo>
          <ogc:Function name="env">
            <ogc:Literal>color</ogc:Literal>
            <ogc:Literal>008000</ogc:Literal>
          </ogc:Function>
          <ogc:Literal>{ color }</ogc:Literal>
        </ogc:PropertyIsEqualTo>
      }
      elseFilter match {
        case Some(x) => {
          <ogc:And>
            { colorFilter(color) }
            { x }
          </ogc:And>
        }
        case _ => {
          colorFilter(color)
        }
      }
    }

    def featureStyleIconMapRule(iconLink: String, filter: Elem): Elem = {
      <Rule>
        <Name>{ iconLink }</Name>
        <ogc:Filter>
          { filter }
        </ogc:Filter>
        <PointSymbolizer>
          <Graphic>
            <ExternalGraphic>
              <OnlineResource xlink:type="simple" xlink:href={ iconLink }/>
              <Format>image/svg+xml</Format>
            </ExternalGraphic>
            <Size>
              <ogc:Function name="env">
                <ogc:Literal>size</ogc:Literal>
                <ogc:Literal>22</ogc:Literal>
              </ogc:Function>
            </Size>
          </Graphic>
        </PointSymbolizer>
      </Rule>
    }

    def featureStyleDefaultIconRule(color: String): Elem = {
      <Rule>
        <Name>{ color }</Name>
        <ogc:Filter>
          { colorWithElseFilter(color)}
        </ogc:Filter>
        <PointSymbolizer>
          <Graphic>
            <ExternalGraphic>
              <OnlineResource xlink:type="simple" xlink:href={ defaultIconURL.replace("[[COLOR]]", color) }/>
              <Format>image/svg+xml</Format>
            </ExternalGraphic>
            <Size>
              <ogc:Function name="env">
                <ogc:Literal>size</ogc:Literal>
                <ogc:Literal>22</ogc:Literal>
              </ogc:Function>
            </Size>
          </Graphic>
        </PointSymbolizer>
      </Rule>
    }

    <StyledLayerDescriptor version="1.0.0" xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd" xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
      <NamedLayer>
        <Name>{name}</Name>
        <UserStyle>
          <Name>{name}</Name>
          <FeatureTypeStyle>
            <Name>Feature Style</Name>
            { for ((iconLink: String, filter: Elem) <- condensedSLDRules) yield featureStyleIconMapRule(iconLink, filter) }
            { for (color <- iconColors) yield featureStyleDefaultIconRule(color) }
          </FeatureTypeStyle>
        </UserStyle>
      </NamedLayer>
    </StyledLayerDescriptor>.toString()
  }

  def registerSurfaceStyles() {
    val highestThreatColor = "#ff0000"
    val lowerThreatColor = "#ffa07a"
    val lowestThreatColor = "#ffffff"
    List(5, 10, 15, 20, 25, 30, 35, 40, 45, 50).foreach(lower => {
      val upper = 1
      registerTwoToneThreatSurfaceStyle(upper, lower)
    })
    List(10, 15, 20, 25, 30, 35, 40, 45, 50).foreach(lower => {
      val upper = 5
      registerTwoToneThreatSurfaceStyle(upper, lower)
    })
    def continuousName = "spinoza_palette_red"
    registerSurfaceStyle(continuousName, continuousSurfaceStyleBody())
    def bftName = "spinoza_palette_bft_density_translucent_blue"
    registerSurfaceStyle(bftName, bftDensitySurfaceStyleBody())
    registerSurfaceStyle(spinozaDefaultName, spinozaAllStyleBody())

    def styleName(upper: Int, lower: Int) = {
      "spinoza_palette_" + pad(upper) + "_" + pad(lower)
    }

    def pad(percentage: Int) = {
      if (percentage < 10) "0" + percentage else String.valueOf(percentage)
    }

    def generateColorMapEntries(upperThreshold: Int, lowerThreshold: Int) = {
      List(ColorMapEntry(lowestThreatColor, 0, (100 - lowerThreshold) * 256 / 100),
        ColorMapEntry(lowerThreatColor, 1, (100 - upperThreshold) * 256 / 100),
        ColorMapEntry(highestThreatColor, 1, 256))
    }

    def registerTwoToneThreatSurfaceStyle(upper: Int, lower: Int) {
      val name = styleName(upper, lower)
      val colorMapEntries = generateColorMapEntries(upper, lower)
      val stringEntity = twoToneThreatSurfaceStyleBody(name, colorMapEntries)
      registerSurfaceStyle(name, stringEntity)
    }

    def registerSurfaceStyle(name: String, stringEntity: String) {
      val method = if (isGlobalStyleRegistered(name)) new HttpPut(s"$globalStylesUrl/$name") else new HttpPost(s"$globalStylesUrl")
      val entity = new StringEntity(stringEntity, ContentType.TEXT_XML)
      method.setEntity(entity)
      method.setHeader(new BasicHeader("Content-type", "application/vnd.ogc.sld+xml"))
      execute(method)
    }

    def spinozaAllStyleBody() = {
      <StyledLayerDescriptor version="1.0.0"
                             xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd"
                             xmlns="http://www.opengis.net/sld"
                             xmlns:ogc="http://www.opengis.net/ogc"
                             xmlns:xlink="http://www.w3.org/1999/xlink"
                             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <NamedLayer>
          <Name>{ spinozaDefaultName }</Name>
          <UserStyle>
            <Title>A boring default style</Title>
            <Name>{ spinozaDefaultName }</Name>
            <FeatureTypeStyle>
              <Rule>
                <Name>pointstyle</Name>
                <ogc:Filter>
                  <ogc:PropertyIsEqualTo>
                    <ogc:Function name="geometryType">
                      <ogc:PropertyName>{ geometryTypeName }</ogc:PropertyName>
                    </ogc:Function>
                    <ogc:Literal>Point</ogc:Literal>
                  </ogc:PropertyIsEqualTo>
                </ogc:Filter>
                <PointSymbolizer>
                  <Graphic>
                    <Mark>
                      <WellKnownName>circle</WellKnownName>
                      <Fill>
                        <CssParameter name="fill">#000000</CssParameter>
                      </Fill>
                    </Mark>
                    <Size>12</Size>
                  </Graphic>
                </PointSymbolizer>
              </Rule>
              <Rule>
                <Name>linestyle</Name>
                <ogc:Filter>
                  <ogc:PropertyIsEqualTo>
                    <ogc:Function name="in5">
                      <ogc:Function name="geometryType">
                        <ogc:PropertyName>{ geometryTypeName }</ogc:PropertyName>
                      </ogc:Function>
                      <ogc:Literal>LineString</ogc:Literal>
                      <ogc:Literal>LinearRing</ogc:Literal>
                      <ogc:Literal>MultiLineString</ogc:Literal>
                      <ogc:Literal>Polygon</ogc:Literal>
                      <ogc:Literal>MultiPolygon</ogc:Literal>
                    </ogc:Function>
                    <ogc:Literal>true</ogc:Literal>
                  </ogc:PropertyIsEqualTo>
                </ogc:Filter>
                <LineSymbolizer>
                  <Stroke>
                    <CssParameter name="stroke">#008000</CssParameter>
                    <CssParameter name="width">1</CssParameter>
                  </Stroke>
                </LineSymbolizer>
              </Rule>
            </FeatureTypeStyle>
          </UserStyle>
        </NamedLayer>
      </StyledLayerDescriptor>.toString
    }

    def bftDensitySurfaceStyleBody() = {
      <sld:StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:sld="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:gml="http://www.opengis.net/gml" version="1.0.0">
        <sld:NamedLayer>
          <sld:Name>{ bftName }</sld:Name>
          <sld:UserStyle>
            <sld:Name>{ bftName }</sld:Name>
            <sld:Title>A default style</sld:Title>
            <sld:Abstract>A sample style for BFT rasters</sld:Abstract>
            <sld:FeatureTypeStyle>
              <sld:Name>name</sld:Name>
              <sld:FeatureTypeName>Feature</sld:FeatureTypeName>
              <sld:Rule>
                <sld:RasterSymbolizer>
                  <sld:Geometry>
                    <ogc:PropertyName>geom</ogc:PropertyName>
                  </sld:Geometry>
                  <sld:ChannelSelection>
                    <sld:GrayChannel>
                      <sld:SourceChannelName>1</sld:SourceChannelName>
                    </sld:GrayChannel>
                  </sld:ChannelSelection>
                  <sld:ColorMap extended="ramp">
                    <sld:ColorMapEntry color="#0000ff" opacity="0" quantity="1"/>
                    <sld:ColorMapEntry color="#0000ff" opacity="1" quantity="256"/>
                  </sld:ColorMap>
                </sld:RasterSymbolizer>
              </sld:Rule>
            </sld:FeatureTypeStyle>
          </sld:UserStyle>
        </sld:NamedLayer>
      </sld:StyledLayerDescriptor>.toString
    }

    def continuousSurfaceStyleBody() = {
      <sld:StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:sld="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:gml="http://www.opengis.net/gml" version="1.0.0">
        <sld:NamedLayer>
          <sld:Name>{ continuousName }</sld:Name>
          <sld:UserStyle>
            <sld:Name>{ continuousName }</sld:Name>
            <sld:Title>A default style</sld:Title>
            <sld:Abstract>A sample style for continuous threat rasters</sld:Abstract>
            <sld:FeatureTypeStyle>
              <sld:Name>name</sld:Name>
              <sld:FeatureTypeName>Feature</sld:FeatureTypeName>
              <sld:Rule>
                <sld:RasterSymbolizer>
                  <sld:Geometry>
                    <ogc:PropertyName>geom</ogc:PropertyName>
                  </sld:Geometry>
                  <sld:ChannelSelection>
                    <sld:GrayChannel>
                      <sld:SourceChannelName>1</sld:SourceChannelName>
                    </sld:GrayChannel>
                  </sld:ChannelSelection>
                  <sld:ColorMap extended="ramp">
                    <sld:ColorMapEntry color="#ffc6c6" opacity="0" quantity="1"/>
                    <sld:ColorMapEntry color="#ffc4c4" opacity="0" quantity="3"/>
                    <sld:ColorMapEntry color="#ffc2c2" opacity="0" quantity="6"/>
                    <sld:ColorMapEntry color="#ffc0c0" opacity="0" quantity="8"/>
                    <sld:ColorMapEntry color="#ffbebe" opacity=".0039" quantity="11"/>
                    <sld:ColorMapEntry color="#ffbcbc" opacity=".0039" quantity="13"/>
                    <sld:ColorMapEntry color="#ffbaba" opacity=".0039" quantity="16"/>
                    <sld:ColorMapEntry color="#ffb8b8" opacity=".0078" quantity="18"/>
                    <sld:ColorMapEntry color="#ffb6b6" opacity=".0078" quantity="21"/>
                    <sld:ColorMapEntry color="#ffb4b4" opacity=".0117" quantity="23"/>
                    <sld:ColorMapEntry color="#ffb2b2" opacity=".0117" quantity="26"/>
                    <sld:ColorMapEntry color="#ffb0b0" opacity=".0156" quantity="29"/>
                    <sld:ColorMapEntry color="#ffaeae" opacity=".0156" quantity="31"/>
                    <sld:ColorMapEntry color="#ffacac" opacity=".0196" quantity="34"/>
                    <sld:ColorMapEntry color="#ffaaaa" opacity=".0235" quantity="36"/>
                    <sld:ColorMapEntry color="#ffa8a8" opacity=".0274" quantity="39"/>
                    <sld:ColorMapEntry color="#ffa6a6" opacity=".0274" quantity="41"/>
                    <sld:ColorMapEntry color="#ffa4a4" opacity=".0313" quantity="44"/>
                    <sld:ColorMapEntry color="#ffa2a2" opacity=".0352" quantity="46"/>
                    <sld:ColorMapEntry color="#ffa0a0" opacity=".0392" quantity="49"/>
                    <sld:ColorMapEntry color="#ff9e9e" opacity=".0431" quantity="52"/>
                    <sld:ColorMapEntry color="#ff9c9c" opacity=".0470" quantity="54"/>
                    <sld:ColorMapEntry color="#ff9a9a" opacity=".0509" quantity="57"/>
                    <sld:ColorMapEntry color="#ff9898" opacity=".0588" quantity="59"/>
                    <sld:ColorMapEntry color="#ff9696" opacity=".0627" quantity="62"/>
                    <sld:ColorMapEntry color="#ff9494" opacity=".0666" quantity="64"/>
                    <sld:ColorMapEntry color="#ff9292" opacity=".0745" quantity="67"/>
                    <sld:ColorMapEntry color="#ff9090" opacity=".0784" quantity="69"/>
                    <sld:ColorMapEntry color="#ff8e8e" opacity=".0823" quantity="72"/>
                    <sld:ColorMapEntry color="#ff8c8c" opacity=".0901" quantity="74"/>
                    <sld:ColorMapEntry color="#ff8a8a" opacity=".0980" quantity="77"/>
                    <sld:ColorMapEntry color="#ff8888" opacity=".1019" quantity="80"/>
                    <sld:ColorMapEntry color="#ff8686" opacity=".1098" quantity="82"/>
                    <sld:ColorMapEntry color="#ff8484" opacity=".1137" quantity="85"/>
                    <sld:ColorMapEntry color="#ff8282" opacity=".1215" quantity="87"/>
                    <sld:ColorMapEntry color="#ff8080" opacity=".1294" quantity="90"/>
                    <sld:ColorMapEntry color="#ff7e7e" opacity=".1372" quantity="92"/>
                    <sld:ColorMapEntry color="#ff7c7c" opacity=".1450" quantity="95"/>
                    <sld:ColorMapEntry color="#ff7a7a" opacity=".1529" quantity="97"/>
                    <sld:ColorMapEntry color="#ff7878" opacity=".1607" quantity="100"/>
                    <sld:ColorMapEntry color="#ff7676" opacity=".1686" quantity="103"/>
                    <sld:ColorMapEntry color="#ff7474" opacity=".1764" quantity="105"/>
                    <sld:ColorMapEntry color="#ff7272" opacity=".1843" quantity="108"/>
                    <sld:ColorMapEntry color="#ff7070" opacity=".1921" quantity="110"/>
                    <sld:ColorMapEntry color="#ff6e6e" opacity=".2039" quantity="113"/>
                    <sld:ColorMapEntry color="#ff6c6c" opacity=".2117" quantity="115"/>
                    <sld:ColorMapEntry color="#ff6a6a" opacity=".2196" quantity="118"/>
                    <sld:ColorMapEntry color="#ff6868" opacity=".2313" quantity="120"/>
                    <sld:ColorMapEntry color="#ff6666" opacity=".2392" quantity="123"/>
                    <sld:ColorMapEntry color="#ff6464" opacity=".2509" quantity="125"/>
                    <sld:ColorMapEntry color="#ff6262" opacity=".2588" quantity="128"/>
                    <sld:ColorMapEntry color="#ff6060" opacity=".2705" quantity="131"/>
                    <sld:ColorMapEntry color="#ff5e5e" opacity=".2823" quantity="133"/>
                    <sld:ColorMapEntry color="#ff5c5c" opacity=".2901" quantity="136"/>
                    <sld:ColorMapEntry color="#ff5a5a" opacity=".3019" quantity="138"/>
                    <sld:ColorMapEntry color="#ff5858" opacity=".3137" quantity="141"/>
                    <sld:ColorMapEntry color="#ff5656" opacity=".3254" quantity="143"/>
                    <sld:ColorMapEntry color="#ff5454" opacity=".3372" quantity="147"/>
                    <sld:ColorMapEntry color="#ff5252" opacity=".3490" quantity="148"/>
                    <sld:ColorMapEntry color="#ff5050" opacity=".3607" quantity="151"/>
                    <sld:ColorMapEntry color="#ff4e4e" opacity=".3725" quantity="154"/>
                    <sld:ColorMapEntry color="#ff4c4c" opacity=".3843" quantity="156"/>
                    <sld:ColorMapEntry color="#ff4a4a" opacity=".3960" quantity="159"/>
                    <sld:ColorMapEntry color="#ff4848" opacity=".4078" quantity="161"/>
                    <sld:ColorMapEntry color="#ff4646" opacity=".4235" quantity="164"/>
                    <sld:ColorMapEntry color="#ff4444" opacity=".4352" quantity="166"/>
                    <sld:ColorMapEntry color="#ff4242" opacity=".4470" quantity="169"/>
                    <sld:ColorMapEntry color="#ff4040" opacity=".4627" quantity="171"/>
                    <sld:ColorMapEntry color="#ff3e3e" opacity=".4745" quantity="174"/>
                    <sld:ColorMapEntry color="#ff3c3c" opacity=".4901" quantity="176"/>
                    <sld:ColorMapEntry color="#ff3a3a" opacity=".5058" quantity="179"/>
                    <sld:ColorMapEntry color="#ff3838" opacity=".5176" quantity="182"/>
                    <sld:ColorMapEntry color="#ff3636" opacity=".5333" quantity="184"/>
                    <sld:ColorMapEntry color="#ff3434" opacity=".5490" quantity="187"/>
                    <sld:ColorMapEntry color="#ff3232" opacity=".5607" quantity="189"/>
                    <sld:ColorMapEntry color="#ff3030" opacity=".5764" quantity="192"/>
                    <sld:ColorMapEntry color="#ff2e2e" opacity=".5921" quantity="194"/>
                    <sld:ColorMapEntry color="#ff2c2c" opacity=".6078" quantity="197"/>
                    <sld:ColorMapEntry color="#ff2a2a" opacity=".6235" quantity="199"/>
                    <sld:ColorMapEntry color="#ff2828" opacity=".6392" quantity="202"/>
                    <sld:ColorMapEntry color="#ff2626" opacity=".6549" quantity="205"/>
                    <sld:ColorMapEntry color="#ff2424" opacity=".6705" quantity="207"/>
                    <sld:ColorMapEntry color="#ff2222" opacity=".6901" quantity="210"/>
                    <sld:ColorMapEntry color="#ff2020" opacity=".7058" quantity="212"/>
                    <sld:ColorMapEntry color="#ff1e1e" opacity=".7215" quantity="215"/>
                    <sld:ColorMapEntry color="#ff1c1c" opacity=".7411" quantity="217"/>
                    <sld:ColorMapEntry color="#ff1a1a" opacity=".7568" quantity="220"/>
                    <sld:ColorMapEntry color="#ff1818" opacity=".7725" quantity="222"/>
                    <sld:ColorMapEntry color="#ff1616" opacity=".7921" quantity="225"/>
                    <sld:ColorMapEntry color="#ff1414" opacity=".8117" quantity="227"/>
                    <sld:ColorMapEntry color="#ff1212" opacity=".8274" quantity="230"/>
                    <sld:ColorMapEntry color="#ff1010" opacity=".8470" quantity="233"/>
                    <sld:ColorMapEntry color="#ff0e0e" opacity=".8666" quantity="235"/>
                    <sld:ColorMapEntry color="#ff0c0c" opacity=".8823" quantity="238"/>
                    <sld:ColorMapEntry color="#ff0a0a" opacity=".9019" quantity="240"/>
                    <sld:ColorMapEntry color="#ff0808" opacity=".9215" quantity="243"/>
                    <sld:ColorMapEntry color="#ff0606" opacity=".9411" quantity="245"/>
                    <sld:ColorMapEntry color="#ff0404" opacity=".9607" quantity="248"/>
                    <sld:ColorMapEntry color="#ff0202" opacity=".9803" quantity="250"/>
                    <sld:ColorMapEntry color="#ff0000" opacity="1.0000" quantity="253"/>
                    <sld:ColorMapEntry color="#ff0000" opacity="1.0000" quantity="256"/>
                  </sld:ColorMap>
                </sld:RasterSymbolizer>
              </sld:Rule>
            </sld:FeatureTypeStyle>
          </sld:UserStyle>
        </sld:NamedLayer>
      </sld:StyledLayerDescriptor>.toString
    }

    def twoToneThreatSurfaceStyleBody(name: String, colorMapEntries: List[ColorMapEntry]) = {
      <sld:StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:sld="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:gml="http://www.opengis.net/gml" version="1.0.0">
        <sld:NamedLayer>
          <sld:Name>{ name }</sld:Name>
          <sld:UserStyle>
            <sld:Name>{ name }</sld:Name>
            <sld:Title>A default style</sld:Title>
            <sld:Abstract>A sample style for rasters</sld:Abstract>
            <sld:FeatureTypeStyle>
              <sld:Name>name</sld:Name>
              <sld:FeatureTypeName>Feature</sld:FeatureTypeName>
              <sld:Rule>
                <sld:RasterSymbolizer>
                  <sld:Geometry>
                    <ogc:PropertyName>geom</ogc:PropertyName>
                  </sld:Geometry>
                  <sld:ChannelSelection>
                    <sld:GrayChannel>
                      <sld:SourceChannelName>1</sld:SourceChannelName>
                    </sld:GrayChannel>
                  </sld:ChannelSelection>
                  <sld:ColorMap type="intervals" extended="intervals">
                    { for (colorMapEntry: ColorMapEntry <- colorMapEntries)
                  yield <sld:ColorMapEntry color={ colorMapEntry.color } opacity={ colorMapEntry.opacity.toString } quantity={ colorMapEntry.quantity.toString }/> }
                  </sld:ColorMap>
                </sld:RasterSymbolizer>
              </sld:Rule>
            </sld:FeatureTypeStyle>
          </sld:UserStyle>
        </sld:NamedLayer>
      </sld:StyledLayerDescriptor>.toString
    }
  }

  case class ColorMapEntry(color: String, opacity: Int, quantity: Int)

  def registerCoverageStore(name: String, url: String) {
    logger.debug(s"Registering coverage store:  name $name; URL $url")
    post(coverageStoresURL,
      ContentType.TEXT_XML,
      coverageStoreBody(name, coverageFormatName, true, config.layersNamespace, url))
  }

  def coverageStoreBody(name: String,
                        formatName: String,
                        enabled: Boolean,
                        workspace: String,
                        url: String) =
    <coverageStore>
      <name>{ name }</name>
      <type>{ formatName }</type>
      <enabled>{ enabled }</enabled>
      <workspace>{ workspace }</workspace>
      <url>{ url }</url>
    </coverageStore>.toString()

  def registerCoverage(storeName: String, name:String=null) {
    logger.debug(s"Registering coverage:  store $storeName; name $name")
    post(s"$coverageStoresURL/$storeName/coverages",
      ContentType.TEXT_XML,
      coverageBody(if(name == null) storeName else name,
        "EPSG:4326",
        GeoserverBoundingBox.WHOLE_EARTH,
        GeoserverBoundingBox.WHOLE_EARTH))
  }

  def coverageBody(name: String,
                   srs: String,
                   nativeBoundingBox: GeoserverBoundingBox,
                   latLonBoundingBox: GeoserverBoundingBox) =
    <coverage>
      <name>{ name }</name>
      <srs>{ srs }</srs>
      <nativeBoundingBox>
        <minx>{ nativeBoundingBox.minx }</minx>
        <maxx>{ nativeBoundingBox.maxx }</maxx>
        <miny>{ nativeBoundingBox.miny }</miny>
        <maxy>{ nativeBoundingBox.maxy }</maxy>
        <crs>{ nativeBoundingBox.crs }</crs>
      </nativeBoundingBox>
      <latLonBoundingBox>
        <minx>{ latLonBoundingBox.minx }</minx>
        <maxx>{ latLonBoundingBox.maxx }</maxx>
        <miny>{ latLonBoundingBox.miny }</miny>
        <maxy>{ latLonBoundingBox.maxy }</maxy>
        <crs>{ latLonBoundingBox.crs }</crs>
      </latLonBoundingBox>
      <metadata>
        <entry key="time">
          <dimensionInfo>
            <enabled>true</enabled>
            <presentation>CONTINUOUS_INTERVAL</presentation>
            <units>ISO8601</units>
          </dimensionInfo>
        </entry>
        <entry key="cachingEnabled">false</entry>
      </metadata>
    </coverage>.toString()

  def modifyDefaultStyle(name: String, style: String) {
    logger.debug(s"Setting default layer style:  name $name; style $style")
    if (style != null) {
      put(s"$layersUrl/${config.layersNamespace}:$name",
        ContentType.TEXT_XML,
        layerBody(true, style))
    }
  }

  def modifyDefaultStyleNonGWC(name: String, style: String) {
    logger.debug(s"Setting default layer style:  name $name; style $style")
    if (style != null) {
      put(s"$layersUrl/${config.layersNamespace}:$name",
        ContentType.TEXT_XML,
        layerBodyNonGWC(true, style))
    }
  }

  def layerBodyNonGWC(enabled: Boolean, styleName: String) =
    <layer>
      <enabled>{ enabled }</enabled>
      <defaultStyle>
        <name>{ styleName }</name>
        <workspace>{ config.layersNamespace }</workspace>
      </defaultStyle>
    </layer>.toString()

  def layerBody(enabled: Boolean, styleName: String) =
    <layer>
      <enabled>{ enabled }</enabled>
      <defaultStyle>
        <name>{ styleName }</name>
      </defaultStyle>
      <styles class="linked-hash-set">
        { gwcStyles.map(s => <style><name>{ s }</name></style>) }
      </styles>
    </layer>.toString()

  /**
   * Depending on whether the " Automatically configure a GeoWebCache layer for each new layer or layer group"
   *  box is ticked in Geoserver, we either use a put or a post.
   *
   *  http://geowebcache.org/docs/current/rest/layers.html
   *
   * @param name               Name of the layer (without the workspace name).
   * @param defaultStyleName   Name of the default style to use with GWC.
   */
  def registerGeoWebCache(name:String, defaultStyleName: String = null) {
    logger.debug(s"Registering GWC: name $name; default style $defaultStyleName")
    val layerUrl = s"$geoWebCacheURL/${config.layersNamespace}:$name.xml"

    val method = if(isGWCRegistered(name)) post _ else put _

    method(
      layerUrl,
      ContentType.TEXT_XML,
      gwcBody(name, defaultStyleName)
    )
  }

  def gwcBody(layerName: String, defaultStyleName: String) =
    <GeoServerLayer>
      <enabled>true</enabled>
      <name>{ config.layersNamespace }:{ layerName }</name>
      <mimeFormats>
        <string>image/jpeg</string>
        <string>image/png</string>
      </mimeFormats>
      <gridSubsets>
        <gridSubset>
          <gridSetName>EPSG:900913</gridSetName>
        </gridSubset>
        <gridSubset>
          <gridSetName>EPSG:4326</gridSetName>
        </gridSubset>
      </gridSubsets>
      <metaWidthHeight>
        <int>4</int>
        <int>4</int>
      </metaWidthHeight>
      <parameterFilters>
        <stringParameterFilter>
          <key>STYLES</key>
          {
          if (defaultStyleName != null) {
            <defaultValue>{ defaultStyleName }</defaultValue>
          }
          }
          <values>
            { gwcStyles.map(s => <string>{ s }</string>) }
          </values>
        </stringParameterFilter>
        <regexParameterFilter>
          <key>TIME</key>
          <defaultValue></defaultValue>
          <regex>.*</regex>
        </regexParameterFilter>
      </parameterFilters>
      <gutter>0</gutter>
      <autoCacheStyles>true</autoCacheStyles>
    </GeoServerLayer>.toString()

  private[this] def post(url: String, contentType: ContentType, xml: String): Int =
    putOrPost(new HttpPost(url), contentType, xml)

  private[this] def put(url: String, contentType: ContentType, xml: String): Int =
    putOrPost(new HttpPut(url), contentType, xml)

  private[this] def putOrPost(method: HttpEntityEnclosingRequestBase, contentType: ContentType, xml: String) = {
    method.setEntity(new StringEntity(xml, contentType))
    method.setHeader(new BasicHeader("Content-type", contentType.getMimeType))
    execute(method)
  }

  private[this] def get(url: String): Int = execute(new HttpGet(url))

  private[this] def delete(url: String): Int = execute(new HttpDelete(url))

  lazy val httpClient = new DefaultHttpClient

  import scala.concurrent.duration._

  private[this] def execute(method: HttpRequestBase): Int = {
    implicit val timeout = Timeout(1 hour)
    Await.result((geoserverRequestQueue ? method).mapTo[Int], 1 hour)
  }

  // since geoserver registration api is not thread safe, we use a single threaded queue to process all geoserver requests
  val geoserverRequestQueue = actorSystem.actorOf(Props(new Actor {
    def receive = {
      case m: HttpRequestBase =>
        try {
          sender ! executeMethod(m)
        } catch {
          case t: Throwable => sender ! Failure(t)
        }
      case m =>
        logger.warn(s"Unrecognized message! $m")
    }

    def executeMethod(method: HttpRequestBase): Int = {
      val bcp = new BasicCredentialsProvider
      bcp.setCredentials(AuthScope.ANY, credentials)
      httpClient.setCredentialsProvider(bcp)
      try {
        val httpResponse = httpClient.execute(method)
        if (httpResponse.getStatusLine.getStatusCode >= 400)
          throw new Exception(
            "Invalid return code:" +
              httpResponse.getStatusLine.getStatusCode +
              " - " +
              EntityUtils.toString(httpResponse.getEntity))
        httpResponse.getStatusLine.getStatusCode
      } finally {
        method.releaseConnection()
      }
    }
  }))

  class RegistrationActor extends Actor {
    def receive = {
      case RegistrationData(url, storeName, coverageName, styleName) => {
        try {
          registerWorkspaceIfNotRegistered
          if (!isCoverageRegistered(coverageName) && !isCoverageRegistered(storeName)) {
            registerCoverageStore(storeName, url)
            registerCoverage(storeName, coverageName)
            modifyDefaultStyle(coverageName, styleName)
            registerGeoWebCache(coverageName, styleName)
          }
          sender ! Done()
        } catch {
          case t: Throwable => sender ! akka.actor.Status.Failure(t)
        }
      }
      case m => logger.warn(s"Unrecognized message! $m")
    }
  }
  case class RegistrationData(url: String, storeName: String, coverageName: String, styleName: String)
  case class Done()
}

case class GeoserverBoundingBox(minx: Double, maxx: Double, miny: Double, maxy: Double, crs: String)
object GeoserverBoundingBox {
  val WHOLE_EARTH = GeoserverBoundingBox(-180.0, 180.0, -90.0, 90.0, "EPSG:4326")
}

object config {
  val geoserverUsername: String = "admin"
  val geoserverPassword: String = "geoserver"
  val layersNamespace: String = "aava"
  val geoserverNamespaceUrl: String = "http://aava"
  val geoserverUrl: String = "http://jw9bn:8080/geoserver"
  val geoserverRegistrationTimeout: FiniteDuration = Duration(120, TimeUnit.SECONDS)
  val gwcDefaultStyles: String = "_palette_01_05,_palette_01_10,_palette_01_15,_palette_01_20,_palette_01_25,_palette_01_30,_palette_01_35,_palette_01_40,_palette_01_45,_palette_01_50,_palette_05_10,_palette_05_15,_palette_05_20,_palette_05_25,_palette_05_30,_palette_05_35,_palette_05_40,_palette_05_45,_palette_05_50,_palette_red"
  val stylesNamespace: String = "spinoza"
  val continuousThreatStyleName: String = stylesNamespace + "_palette_red"
}
