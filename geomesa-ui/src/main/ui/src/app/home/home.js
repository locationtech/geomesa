angular.module('geomesa.home', [
    'geomesa.masthead',
    'geomesa.map',
    'geomesa.card'
])
    .config(['$routeProvider', function ($routeProvider) {
        $routeProvider.when('/home', {
            templateUrl: 'home/home.tpl.html'
        });
    }])

    .controller('HomeController', ['$scope', function($scope) {
        $scope.cql = '';
        $scope.mapAPI = {};

        $scope.selectedPoint = {};

        $scope.$watch('cql', function (cqlFilter) {
            if (cqlFilter) {
                $scope.mapAPI.applyCQL(cqlFilter);
            }
        });
    }]);
