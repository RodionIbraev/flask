    function NormTimelineCtrl($scope) {
        $(document).ready(function(){
            $scope.intersections_names = {};
            let SHASIntervals = {{ SHAS_intervals|safe }};
            let PDMIntervals = {{ PDM_intervals|safe }};
            let SBUIntervals = {{ SBU_intervals|safe }};
            
            function searchIntersections(intervals, vehicleType) {
                if (!$scope.intersections_names[vehicleType]) {
                    $scope.intersections_names[vehicleType] = [];
                }
                
                $.each(intervals, function(index, interval) {
                    if (interval.intersection && $scope.intersections_names[vehicleType].indexOf(interval.idle_type__name) == -1){
                        $scope.intersections_names[vehicleType].push(interval.idle_type__name);
                    }
                });
            }
            
            searchIntersections(SHASIntervals, "ШАС");
            searchIntersections(PDMIntervals, "ПДМ");
            searchIntersections(SBUIntervals, "СБУ");

            $('#onLoadModalWindow').modal('show');
        });
