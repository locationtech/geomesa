angular.module('geomesa.map', [])

    .directive('geomesaMap', [function () {
        return {
            restrict: 'E',
            scope: {
                map: '=?',
                api: '=',
                selectedPoint: '='
            },
//            link: function (scope, element, attrs) {
//                //var baseLayer = L.tileLayer.provider('Stamen.TonerLite'),
//                var baseLayer = L.tileLayer.provider('MapQuestOpen.OSM'), 
//                    wmsLayer = L.tileLayer.wms("http://geomesa:8080/geoserver/geomesa/wms", {
//                        layers: 'geomesa:QuickStart',
//                        format: 'image/png',
//                        transparent: true
//                    });
//
//                scope.map = L.map(element[0], {
//                    center: L.latLng(-38.09, -76.85),
//                    zoom: 8,
//                    maxZoom: 18,
//                    minZoom: 3,
//                    attributionControl: false,
//                    layers: [baseLayer, wmsLayer]
//                });
//
//                scope.map.on('click', function (evt) {
//                    scope.$apply(function () {
//                        scope.selectedPoint = {
//                            lat: evt.latlng.lat,
//                            lng: evt.latlng.lng
//                        };
//                    });
//                });
//
//                scope.api = {
//                    applyCQL: function (cql) {
//                        console.log(cql);
//                    }
//                };
//
//            }

            link: function (scope, element, attrs) {
               //var baseLayer = L.tileLayer.provider('Stamen.TonerLite'),
            // map = new OpenLayers.Map( 'map' );
            // layer = new OpenLayers.Layer.WMS( "OpenLayers WMS",
            //         "http://vmap0.tiles.osgeo.org/wms/vmap0",
            //         {layers: 'basic'} );
            // map.addLayer(layer);
            // map.zoomToMaxExtent();

                var baseLayer = new ol.layer.Tile({
                   source: new ol.source.MapQuest({layer: 'osm'})
                });
                var wmsLayer = new ol.layer.Tile({
                    source: new ol.source.TileWMS({
                      url: 'http://geomesa:8080/geoserver/geomesa/wms',
                      params: {LAYERS: 'basic'} //QuickStart //when using the actual layer, the layer get screwed up
                    })
                });

                 scope.map = new ol.Map({
                   target: element[0],
                   layers: [baseLayer, wmsLayer],
                   view: new ol.View({
                     center: ol.proj.transform([37.41, 8.82], 'EPSG:4326', 'EPSG:3857'),
                     zoom : 4,
                     maxResolution : 100075016.68557849 / screen.width,
                     minZoomLevel: 3, 
                     maxZoomLevel: 18
                     //maxResolution: "auto",
                   })
                 });
            }
        };
    }]);
