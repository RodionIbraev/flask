<div id="NormTimelineModal" class="modal fade" style="z-index: 1060;">
        <div class="model-dialog" role="document">
            <div class="modal-content">
                <div class="modal-header">
                    <span class="modal-title"><strong ng-bind="title"></strong></span>
                    <button type="button" class="close" data-dismiss="modal"><span aria-hidden="true">×</span><span class="sr-only">Close</span></button>
                </div>
                <div class="modal-body" style="text-align: center;">
                    <div class="row-b5 form-group">
                        <div class="row-b5 form-group center-block" style="display: flex; justify-content: space-between; margin-bottom: 0px;">
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
                </div>
                <div class="modal-footer" style="margin-top: 0;">
                    <div class="align-items-center-b5" style="display: flex; justify-content: space-between;">
                        <div class="legend" style="align-self: flex-start;">
                            <div class="legend-rect" style="background-color: #FF5A5D;"></div>
                            <label class="legend-label" style="color: #808080;">{{ _('Нормы с пересечением') }}</label>
                        </div>
                    </div>
                    <div class="align-items-center-b5" style="display: flex; justify-content: space-between;">
                        <div class="legend" style="align-self: flex-start;">
                            <div class="legend-rect" style="background-color: blue;"></div>
                            <label class="legend-label" style="color: #808080;">{{ _('Свободные интервалы для создания норм') }}</label>
                        </div>
                        <div style="align-self: flex-end;">
                        <button title='{{ _("Закрыть") }}' type="button" data-dismiss="modal"
                                style='outline: none; padding: 0; border: 0px solid #97a4cb; color: inherit; background: none;'>
                            <span>{{ _("Закрыть") }}</span>
                        </button>
                        </div>
                    </div>
                </div>                
            </div>
        </div>
    </div>
