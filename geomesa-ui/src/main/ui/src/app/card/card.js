angular.module('geomesa.card', [])

    .directive('cards', ['WFSResource', function (WFSResource) {
        return {
            restrict: 'E',
            scope: {
                selectedPoint: '='
            },
            templateUrl: 'card/card.tpl.html',
            link: function (scope, element, attrs) {
                scope.cards = [
                    {
                        id: 'testid',
                        name: 'testname',
                        description: 'testDescription'
                    },
                    {
                        id: 'id',
                        name: 'name',
                        description: 'description'
                    }
                ];

                scope.selectedIndex = 0;
                scope.updateIndex = function (i) {
                    scope.selectedIndex = Math.max(Math.min(scope.cards.length - 1, scope.selectedIndex + i), 0);
                };
                function success (response) {
                    console.log(response);
                }

                scope.$watch('selectedPoint', function (p) {
                    if (p.lat && p.lng) {
                        params = {
                            REQUEST: "GetFeatureInfo",
                            EXCEPTIONS: "application/vnd.ogc.se_xml",
                            BBOX: (p.lng - 0.5) + ',' + (p.lat + 0.5) + ',' + (p.lng + 0.5) + ',' + (p.lat - 0.5),
                            SERVICE: "WMS",
                            INFO_FORMAT: 'application/json',
                            QUERY_LAYERS: 'geomesa:QuickStart',
                            FEATURE_COUNT: 50,
                            Layers: 'geomesa:QuickStart',
                            HEIGHT: 300,
                            WIDTH: 300,
                            srs: 'EPSG:4326',
                            version: '1.1.1',
                            x: p.lng,
                            y: p.lat
                        };
                        WFSResource.wfsRequest(params).$promise.then(success);
                    }
                }, true);
            }
        };
    }]);
