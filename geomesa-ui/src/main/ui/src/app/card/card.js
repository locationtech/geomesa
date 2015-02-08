angular.module('geomesa.card', [])

    .directive('cards', [function () {
        return {
            restrict: 'E',
            scope: {
                cards: '='
            },
            templateUrl: 'card/card.tpl.html',
            link: function (scope, element, attrs) {
                scope.$watch('cards', function (p) {
                    scope.selectedIndex = 0;
                });
                scope.updateIndex = function (i) {
                    scope.selectedIndex = Math.max(Math.min(scope.cards.length - 1, scope.selectedIndex + i), 0);
                };
            }
        };
    }]);
