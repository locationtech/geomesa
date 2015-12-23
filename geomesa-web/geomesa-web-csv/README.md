# How to use csv upload from cURL

## Upload a CSV file to the csv endpoint and get a UUID in response
    > curl -F csvfile=@test.csv [GEOSERVER_ROOT]/geomesa/csv

    [CSVID]

## check inferred name and type schema
    > curl [GEOSERVER_ROOT]/geomesa/csv/types/[CSVID]

    test3772907466880411288
    lat:Double, lon:Double, time:String

## update the name and type schema
    > curl -X POST --data "latField=lat&lonField=lon&schema=lat%3ADouble%2C%20lon%3ADouble%2C%20time%3ADate%2C*%20geometry%3APoint%3Asrid%3D4326%3Aindex%3Dtrue" [GEOSERVER_ROOT]/geomesa/csv/types/update/[CSVID]

    test3772907466880411288
    lat:Double, lon:Double, time:String

## gml is available for your csv
    > curl [GEOSERVER_ROOT]/geomesa/csv/[CSVID].gml
    
    <wfs:FeatureCollection ... >

## an xsd is also created (and referenced in the gml)

    > curl [GEOSERVER_ROOT]/geomesa/csv/[CSVID].xsd

    <xsd:schema ... >

## Create request.xml
    <?xml version="1.0" encoding="UTF-8"?>
    <wps:Execute version="1.0.0" service="WPS"
        xmlns="http://www.opengis.net/wps/1.0.0"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:wps="http://www.opengis.net/wps/1.0.0"
        xmlns:ows="http://www.opengis.net/ows/1.1"
        xmlns:xlink="http://www.w3.org/1999/xlink"
        xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsAll.xsd">

        <ows:Identifier>geomesa:Import</ows:Identifier>
        <wps:DataInputs>
            <wps:Input>
                <ows:Identifier>features</ows:Identifier>
                <wps:Reference mimeType="text/xml; subtype=wfs-collection/1.0" xlink:href="[GEOSERVER_ROOT]/geomesa/csv/[CSVID].gml" method="GET"/>
            </wps:Input>
            <wps:Input>
                <ows:Identifier>workspace</ows:Identifier>
                <wps:Data>
                    <wps:LiteralData>[WORKSPACE]</wps:LiteralData>
                </wps:Data>
            </wps:Input>
            <wps:Input>
                <ows:Identifier>store</ows:Identifier>
                <wps:Data>
                    <wps:LiteralData>[DATASTORE]</wps:LiteralData>
                </wps:Data>
            </wps:Input>
            <wps:Input>
                <ows:Identifier>name</ows:Identifier>
                <wps:Data>
                    <wps:LiteralData>[FEATURENAME]</wps:LiteralData>
                </wps:Data>
            </wps:Input>
        </wps:DataInputs>
        <wps:ResponseForm>
            <wps:RawDataOutput>
                <ows:Identifier>layerName</ows:Identifier>
            </wps:RawDataOutput>
        </wps:ResponseForm>
    </wps:Execute>
    
## Ingest to Geoserver
    curl -X POST --data "@request.xml" -H "Content-type: text/xml" http://localhost:9090/geoserver/ows?strict=true

## Delete CSV and shapefile when they're no longer needed
    > curl -X DELETE [GEOSERVER_ROOT]/geomesa/csv/[CSVID].csv
    > curl [GEOSERVER_ROOT]/geomesa/csv/types/[CSVID].csv

    java.util.NoSuchElementException: key not found: [CSVID]
