angular.module('geomesa', [
    'ngRoute',
    'ngResource',
    'templates-app',
    'geomesa.home'
])

    .factory('WFSResource', ['$resource', function ($resource) {
        return $resource('http://geomesa:8080/geoserver/geomesa/wms', {}, {
            wfsRequest: {method: 'GET', isArray: true}
        });
    }])

    .config(['$routeProvider', function ($routeProvider) {
        // Configure route provider to transform any undefined hashes to /home.
        $routeProvider.otherwise({redirectTo: '/home'});
    }])

    .constant('appInfo', {
        name: 'geomesa',
        title: 'Geomesa'
    })

    .controller('AppController', ['$scope', 'appInfo', function ($scope, appInfo) {
        $scope.appModel = {
            appName: appInfo.name,
            appTitle: appInfo.title
        };
    }]);
