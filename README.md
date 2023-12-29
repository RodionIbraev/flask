<div class="row-b5 form-group">
                        <div class="row-b5 form-group center-block" style="display: flex; margin-bottom: 0px;">
                            <div class="col-3-b5"><label>{{ _("Начало") }}</label></div>
                            <div class="col-3-b5" style="width: auto;"><label>{{ _("Вид простоя") }}</label></div>
                            <div class="col-3-b5"><label>{{ _("Конец") }}</label></div>
                        </div>
                        <div class="row-b5 form-group center-block" style="margin-top: 2px; margin-bottom: 2px; display: flex;" ng-repeat="interval in intervals">
                            <div class="col-3-b5 center-block">
                                <input ng-style="{width: '100%', textAlign: 'center', color: interval.intersection ? 'red' : interval.idle_type__name === 'Свободный интервал' ? 'blue' : 'inherit'}" min-value="range_min" max-value="range_max" ng-value="formatTime(interval.time_begin)" disabled>
                            </div>
                            <div class="col-3-b5 center-block" style="width: auto;">
                                <input ng-style="{width: 'auto', textAlign: 'center', color: interval.intersection ? 'red' : interval.idle_type__name === 'Свободный интервал' ? 'blue' : 'inherit',wordWrap: 'break-word'}" min-value="range_min" max-value="range_max" ng-value="interval.idle_type__name" disabled>
                            </div>
                            <div class="col-3-b5 center-block">
                                <input ng-style="{width: '100%', textAlign: 'center', color: interval.intersection ? 'red' : interval.idle_type__name === 'Свободный интервал' ? 'blue' : 'inherit'}" min-value="range_min" max-value="range_max" ng-value="formatTime(interval.time_end)" disabled>
                            </div>
                        </div>
                    </div> 
