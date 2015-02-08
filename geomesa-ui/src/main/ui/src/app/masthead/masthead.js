angular.module('geomesa.masthead', [])

    .directive('masthead', [function () {
        return {
            restrict: 'E',
            scope: {
                cql: '='
            },
            templateUrl: 'masthead/masthead.tpl.html',
            link: function (scope, element, attrs) {
                scope.cqlHolder = '';
                scope.submitCQL = function () {
                    scope.cql = scope.cqlHolder;
                };
            }
        };
    }]);