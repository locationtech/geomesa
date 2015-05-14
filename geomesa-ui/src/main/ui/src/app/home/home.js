angular.module('geomesa.home', [
    'geomesa.masthead',
    'geomesa.map',
    'geomesa.card',
    'geomesa.unique'
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

        $scope.$watch('cql', function (cqlFilter){
            if ($scope.mapAPI.applyCQL){
                $scope.mapAPI.applyCQL(cqlFilter);
            }
        });
    }]);
