angular.module('geomesa.map', [])

    .directive('geomesaMap', ['$http', function ($http) {

        return {
            restrict: 'E',
            scope: {
                map: '=?',
                api: '=',
                selectedFeatures: '=',
            },

            link: function (scope, element, attrs) {
                scope.selectedFeatures = [
                    {
                        Start: ' Click a data point to view its attributes.'
                    }
                ];

                var projection = ol.proj.get('EPSG:3857'),
                    extent = projection.getExtent(),
                    baseLayer = new ol.layer.Tile({
                        extent: extent,
                        source: new ol.source.MapQuest({layer: 'osm'})
                    }),
                    wmsSource = new ol.source.TileWMS({
                        url: 'http://geomesa:8080/geoserver/geomesa/wms',
                        params: {LAYERS: 'QuickStart'}
                    }),
                        wmsLayer = new ol.layer.Tile({
                        extent: extent,
                        source: wmsSource
                    }),
                    olView = new ol.View({
                        // center: ol.proj.transform([37.41, 8.82], 'EPSG:4326', 'EPSG:3857'),
                        center:[-8554902.86746,-4592147.60759],
                        zoom : 4,
                        maxResolution : 40075016.68557849 / screen.width,
                    });

                scope.map = new ol.Map({
                    target: element[0],
                    layers: [baseLayer, wmsLayer],
                    view: olView
                });

                scope.api = {
                    applyCQL: function (cql) {
                        console.log(cql);
                    }
                };

                scope.map.on('singleclick', function(evt) {
                    var viewResolution = olView.getResolution(),
                        url = wmsSource.getGetFeatureInfoUrl(
                            evt.coordinate, viewResolution, 'EPSG:3857',
                            {'INFO_FORMAT': 'application/json', FEATURE_COUNT: 50}
                    );
                    $http.get(url).success(function(data, status, headers, config) {
                        if (data.features.length){
                            scope.cards = data.features;
                        }
                    }).error(function(data, status, headers, config) {
                        console.log('Error getting data with getFeatureInfo.');
                    });
                }); 
            }
        };
    }]);
