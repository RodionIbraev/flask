    def process_fact_idle(self, result: Result, idle: Idle, planned: bool) -> None:
        duration = idle.duration
        idle_type = idles_cache.get_idle_type(idle.reason_id)
        if duration > dt.timedelta(minutes=settings.MIN_IDLE_DURATION):
            if not idle.reason_id or idle_type is None:
                    result.undefined_idles_fact += duration
            else:
                category = self.get_idle_type_category(idle_type)
                if idle_type.analytic_category_id:
                    if category == IdleCategory.TECHNICAL:
                        if planned:
                            result.idles_technical_planned_fact += duration
                            if self.is_fullshift_idle(idle):
                                result.idles_technical_planned_fullshift_idle_fact += duration
                        else:
                            result.idles_technical_unplanned_fact += duration
                            if self.is_fullshift_idle(idle):
                                result.idles_technical_unplanned_fullshift_idle_fact += duration
                            else:
                                result.idles_technical_unplanned_innershift_idle_fact += duration
        
                        result.total_technical_idles_fact += duration
                    else:
                        if planned:
                            result.idles_organization_planned_fact += duration
                            if self.is_fullshift_idle(idle):
                                result.idles_organization_planned_fullshift_idle_fact += duration
                        else:
                            result.idles_organization_unplanned_fact += duration
                            if self.is_fullshift_idle(idle):
                                result.idles_organization_unplanned_fullshift_idle_fact += duration
                            else:
                                result.idles_organization_unplanned_innershift_idle_fact += duration
        
                        result.total_organization_idles_fact += duration

                    if (
                        idle_type.organization_category_id 
                        and idle_type.organization_category.code == self.STOP_FAILURE_IDLETYPE_ORG_CATEGORY_CODE
                    ):
                        result.idles_technical_stop_failure_duration += duration
                        result.idles_technical_stop_failure_number += 1
                else:
                    pass

                if idle_type.code in self.NO_WORK_IDLE_TYPE_CODES:
                    result.no_work_duration += duration
                if idle_type.code == self.RESERVE_IDLE_TYPE_CODE:
                    result.reserve_duration += duration
            # Посчитаем длительность простоя в общей статистике
            result.idle_type_durations[idle.reason_id]['fact_duration'] += idle.duration
