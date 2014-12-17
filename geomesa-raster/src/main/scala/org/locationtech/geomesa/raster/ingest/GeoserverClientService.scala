/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.raster.ingest

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.methods._
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{BasicCredentialsProvider, DefaultHttpClient}
import org.apache.http.message.BasicHeader
import org.apache.http.params.{BasicHttpParams, HttpConnectionParams, HttpParams}
import org.apache.http.util.EntityUtils
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams

/**
 * This class provides service for registering rasters onto Geoserver.
 *
 * @param config Configurations contains info for connecting to Geoserver and info
 *   of building connection to data source used by geoserver plugin to get coverage data.
 */
class GeoserverClientService(config: Map[String, String]) extends Logging {
  val credentials = new UsernamePasswordCredentials(config("user"), config("password"))
  val geoserverUrl = config("url")
  val namespace = config("namespace")
  val restURL = s"$geoserverUrl/rest"
  val namespaceUrl = s"http://$namespace"
  val layersUrl = s"$restURL/layers"
  val coverageStoresURL = s"$restURL/workspaces/$namespace/coveragestores"
  val geoWebCacheURL = s"$geoserverUrl/gwc/rest/layers"
  val defaultStyleName: String = "default_palette_red"
  val geoserverRegistrationTimeout: Int = 10000
  val globalStylesUrl = s"$restURL/styles"

  def registrationData(rasterId: String,
                       rasterName: String,
                       timeStamp: Long,
                       geohash: String,
                       iRes: Int,
                       styleName: String):
  RegistrationData = {
    val url = coverageURL(rasterId, rasterName, timeStamp, geohash, iRes, config)
    val coverageName = rasterId
    val storeName = config(IngestRasterParams.TABLE) + ":" + coverageName
    RegistrationData(url, storeName, coverageName, styleName)
  }

  def coverageURL(rasterId: String,
                  rasterName: String,
                  timeStamp: Long,
                  geohash: String,
                  iRes: Int,
                  params: Map[String, String]): String = {
    val zookeepers = params(IngestRasterParams.ZOOKEEPERS)
    val instance = params(IngestRasterParams.ACCUMULO_INSTANCE)
    val tableName = params(IngestRasterParams.TABLE)
    val user = params(IngestRasterParams.ACCUMULO_USER)
    val password = params(IngestRasterParams.ACCUMULO_PASSWORD)
    val auths = params(IngestRasterParams.AUTHORIZATIONS)

    s"accumulo://$user:$password@$instance/$tableName#geohash=$geohash#resolution=$iRes" +
      s"#timeStamp=$timeStamp#rasterName=$rasterName#zookeepers=$zookeepers#auths=$auths"
  }

  def registerRaster(rasterId: String,
                     rasterName: String,
                     timeStamp: Long,
                     title: String,
                     description: String,
                     geohash: String,
                     iRes: Int,
                     styleName: Option[String]) {
    val regData = registrationData(rasterId,
                                   rasterName,
                                   timeStamp,
                                   geohash,
                                   iRes,
                                   styleName.getOrElse(defaultStyleName))
    regData match {
      case RegistrationData(url, storeName, coverageName, styleName) =>
        registerWorkspaceIfNotRegistered
        if (!isCoverageRegistered(coverageName) && !isCoverageRegistered(storeName)) {
          logger.debug(s"Register raster layer ($url) to Geoserver: $geoserverUrl")
          registerCoverageStore(storeName, url)
          registerCoverage(storeName, title, description, coverageName)
          modifyDefaultStyle(coverageName, styleName)
        }
    }
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
    if (!isWorkspaceRegistered(namespace)) registerWorkspace(namespace)
  }

  def isWorkspaceRegistered(name: String): Boolean =
    doesGetReturn(s"$restURL/workspaces/$name",
      s"Workspace is registered: $name",
      s"Workspace is NOT registered: $name")

  def isCoverageRegistered(name: String): Boolean =
    doesGetReturn(s"$coverageStoresURL/$name",
      s"Coverage is registered:  $name",
      s"Coverage is NOT registered:  $name")

  def isGlobalStyleRegistered(name: String): Boolean =
    doesGetReturn(s"$globalStylesUrl/$name",
      s"Global Style is registered:  $name",
      s"Global Style is NOT registered:  $name")

  def registerRasterStyles() {
    def defaultStyleName = "default_palette_red"
    registerRasterStyle(defaultStyleName, defaultStyleBody())

    def registerRasterStyle(styleName: String, stringEntity: String) {
      val method = if (isGlobalStyleRegistered(styleName)) new HttpPut(s"$globalStylesUrl/$styleName")
                   else new HttpPost(s"$globalStylesUrl")
      val entity = new StringEntity(stringEntity, ContentType.TEXT_XML)
      method.setEntity(entity)
      method.setHeader(new BasicHeader("Content-type", "application/vnd.ogc.sld+xml"))
      execute(method)
    }

    def defaultStyleBody() = {
      <sld:StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:sld="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:gml="http://www.opengis.net/gml" version="1.0.0">
        <sld:NamedLayer>
          <sld:Name>{ defaultStyleName }</sld:Name>
          <sld:UserStyle>
            <sld:Name>{ defaultStyleName }</sld:Name>
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
  }

  def registerCoverageStore(name: String, url: String) {
    logger.debug(s"Registering coverage store:  name $name; URL $url")
    post(coverageStoresURL,
      ContentType.TEXT_XML,
      coverageStoreBody(name, GeoserverClientService.coverageFormatName, true, namespace, url))
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

  def registerCoverage(storeName: String, title:String, description: String, name:String=null) {
    logger.debug(s"Registering coverage:  store $storeName; name $name")
    post(s"$coverageStoresURL/$storeName/coverages",
      ContentType.TEXT_XML,
      coverageBody(if(name == null) storeName else name,
        title,
        description,
        "EPSG:4326",
        GeoserverBoundingBox.WHOLE_EARTH,
        GeoserverBoundingBox.WHOLE_EARTH))
  }

  def coverageBody(name: String,
                   title: String,
                   description: String,
                   srs: String,
                   nativeBoundingBox: GeoserverBoundingBox,
                   latLonBoundingBox: GeoserverBoundingBox) =
    <coverage>
      <title>{ title }</title>
      <abstract>{ description }</abstract>
      <name>{ name }</name>
      <srs>{ srs }</srs>
      <nativeFormat> { GeoserverClientService.coverageFormatName }</nativeFormat>
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
      <parameters>
        <entry>
          <string>RESOLUTION</string>
          <string>1.0</string>
        </entry>
      </parameters>
    </coverage>.toString()

  def modifyDefaultStyle(name: String, style: String) {
    logger.debug(s"Setting default layer style:  name $name; style $style")
    if (style != null) {
      put(s"$layersUrl/${namespace}:$name",
        ContentType.TEXT_XML,
        layerBody(true, style))
    }
  }

  def layerBody(enabled: Boolean, styleName: String) =
    <layer>
      <enabled>{ enabled }</enabled>
      <defaultStyle>
        <name>{ styleName }</name>
      </defaultStyle>
      <styles class="linked-hash-set">
        <style><name>{ defaultStyleName }</name></style>
      </styles>
    </layer>.toString()

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

  lazy val httpClient = {
    val httpParams: HttpParams = new BasicHttpParams
    HttpConnectionParams.setConnectionTimeout(httpParams, geoserverRegistrationTimeout)
    new DefaultHttpClient(httpParams)
  }

  def execute(method: HttpRequestBase): Int = {
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
    } catch {
      case e: Exception =>
        throw new Exception("Attempt of registering raster to geoserver failed: " + e)
    } finally {
      method.releaseConnection()
    }

  }
}

object GeoserverClientService {
  val coverageFormatName = "Geomesa Coverage Format"
}

case class RegistrationData(url: String, storeName: String, coverageName: String, styleName: String)

case class GeoserverBoundingBox(minx: Double, maxx: Double, miny: Double, maxy: Double, crs: String)

object GeoserverBoundingBox {
  val WHOLE_EARTH = GeoserverBoundingBox(-180.0, 180.0, -90.0, 90.0, "EPSG:4326")
}
