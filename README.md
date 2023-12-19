class SDOGoingOnworkInterface(BaseGoingOnworkInterface):
    truck_efficiency_period_class = TruckEfficiencyPeriod

    def __init__(self, vehicle_ids, enterprise, date_start, date_end, start_shift, end_shift, use_saved_facts=False):
        trucks = Truck.objects.filter(id__in=vehicle_ids, is_bench=False).select_related("vehicle_type")
        self.trucks_ids_by_vehicle_type = defaultdict(list)

        for truck in trucks:
            self.trucks_ids_by_vehicle_type[truck.vehicle_type].append(truck.id)

        super().__init__(vehicle_ids, enterprise, date_start, date_end, start_shift, end_shift, use_saved_facts)

    @cached_property
    def trucks(self) -> Dict[int, Truck]:
        return {x.id: x for x in Truck.objects.get_all().filter(pk__in=self.vehicle_ids)}

    @staticmethod
    def get_basic_row_for_efficiency_data(
        efficiency_data: TruckEfficiencyPeriod, total_row: bool = False
    ) -> SdoGoingOnworkReportRow:
        def to_td(value):
            return dt.timedelta(seconds=value)

        row = SdoGoingOnworkReportRow(
            calendar_fond_time_group=to_td(efficiency_data.total_time),
            going_onwork_plan_group__available_on_shift_start=round(
                efficiency_data.going_onwork_plan_available_on_shift_start, 2
            ),
            going_onwork_plan_group__planning_on_shift_start=round(
                efficiency_data.going_onwork_plan_planning_on_shift_start, 2
            ),
            going_onwork_fact_group__technical_potential=round(
                efficiency_data.going_onwork_fact_technical_potential, 2
            ),
            going_onwork_fact_group__actual=round(efficiency_data.going_onwork_fact_actual, 2),
            hourly_workload_group__plan=round(efficiency_data.hourly_workload_group__plan, 2),
            hourly_workload_group__fact=round(efficiency_data.hourly_workload_group__fact, 2),
            operation_time_group__plan=to_td(efficiency_data.operative_time_plan),
            operation_time_group__fact=to_td(efficiency_data.operative_time_fact),
            idles_sum_group__plan=to_td(efficiency_data.idles_technical_plan + efficiency_data.idles_organization_plan),
            idles_sum_group__fact=to_td(
                efficiency_data.idles_technical_fact
                + efficiency_data.idles_organization_fact
                + efficiency_data.undefined_idles_fact
            ),
            undefined_idles_group__fact=to_td(efficiency_data.undefined_idles_fact),
            idles_technical__total_group__plan=to_td(efficiency_data.idles_technical_plan),
            idles_technical__total_group__fact=to_td(efficiency_data.idles_technical_fact),
            idles_technical__planned_group__plan=to_td(efficiency_data.idles_technical_planned_plan),
            idles_technical__planned_group__fact=to_td(efficiency_data.idles_technical_planned_fact),
            idles_technical__unplanned_group__fullshift_fact=to_td(
                efficiency_data.idles_technical_unplanned_fullshift_idle_fact
            ),
            idles_technical__unplanned_group__innershift_fact=to_td(
                efficiency_data.idles_technical_unplanned_innershift_idle_fact
            ),
            idles_organization__total_group__plan=to_td(efficiency_data.idles_organization_plan),
            idles_organization__total_group__fact=to_td(efficiency_data.idles_organization_fact),
            idles_organization__planned_group__plan=to_td(efficiency_data.idles_organization_planned_plan),
            idles_organization__planned_group__fact=to_td(efficiency_data.idles_organization_planned_fact),
            idles_organization__unplanned_group__fullshift_fact=to_td(
                efficiency_data.idles_organization_unplanned_fullshift_idle_fact
            ),
            idles_organization__unplanned_group__innershift_fact=to_td(
                efficiency_data.idles_organization_unplanned_innershift_idle_fact
            ),
            ktg__plan=efficiency_data.ktg_plan,
            ktg__fact=efficiency_data.ktg_fact,
            kio__plan=efficiency_data.kio_plan,
            kio__fact=efficiency_data.kio_fact,
            kio_end_to_end__plan=efficiency_data.kio_end_to_end_plan,
            kio_end_to_end__fact=efficiency_data.kio_end_to_end_fact_v2,
            total_trips__plan=efficiency_data.total_trips_plan,
            total_trips__fact=efficiency_data.total_trips_fact,
            is_total_row=total_row,
        )
        return row

    def get_raw_dateshift_data(self) -> List[TruckEfficiencyModel]:
        raw_dateshift_data = self.truck_efficiency_period_class.get_raw_dateshift_data(
            list(self.trucks.values()), self.context, classReportSource=LightReportSource
        )
        return raw_dateshift_data

    @cached_property
    def dateshift_data_by_truck(self) -> Dict[int, List[TruckEfficiencyModel]]:
        """Структурируем данные по самосвалам в удобочитаемый словарь"""
        dateshift_data = self.dateshift_data[0]
        return self.dateshift_data_by_vehicle(dateshift_data)

    def get_row_for_vehicle(self, vehicle_id) -> SdoGoingOnworkReportRow:
        truck = self.trucks[vehicle_id]  # type: Truck
        if truck.start_expl_date:
            operation_start_date = truck.start_expl_date.astimezone(self.context.tz).year
        else:
            operation_start_date = None
        truck_data = self.dateshift_data_by_truck[truck.id]
        efficiency_data = self.truck_efficiency_period_class(truck_data)
        row = self.get_basic_row_for_efficiency_data(efficiency_data)
        row.vehicle_name = truck.name
        row.operation_start_date = operation_start_date
        return row

    def get_total_row(self, rows: List[SdoGoingOnworkReportRow], total_name: str = "Итого") -> SdoGoingOnworkReportRow:
        from typing import TypeVar

        T = TypeVar("T")

        def sum_(rows_: List, field_name: str, start: T = 0, ndigits: Union[int, None] = None) -> T:
            result = sum((getattr(row, field_name) for row in rows_), start)
            return round(result, ndigits) if ndigits else result

        def avg(rows_: list, field_name: str, ndigits: Union[int, None] = None, exclude_zero: bool = False):
            values = [getattr(row, field_name) for row in rows_ if not exclude_zero or getattr(row, field_name) != 0]
            if not values:
                return 0
            result = sum(values) / len(values)
            return round(result, ndigits) if ndigits else result

        total_row = SdoGoingOnworkReportRow(
            calendar_fond_time_group=sum_(rows, "calendar_fond_time_group", start=dt.timedelta(0)),
            going_onwork_plan_group__available_on_shift_start=sum_(
                rows, "going_onwork_plan_group__available_on_shift_start", ndigits=2
            ),
            going_onwork_plan_group__planning_on_shift_start=sum_(
                rows, "going_onwork_plan_group__planning_on_shift_start", ndigits=2
            ),
            going_onwork_fact_group__technical_potential=sum_(
                rows, "going_onwork_fact_group__technical_potential", ndigits=2
            ),
            going_onwork_fact_group__actual=sum_(rows, "going_onwork_fact_group__actual", ndigits=2),
            hourly_workload_group__plan=avg(rows, "hourly_workload_group__plan", ndigits=2, exclude_zero=True),
            hourly_workload_group__fact=avg(rows, "hourly_workload_group__fact", ndigits=2, exclude_zero=True),
            operation_time_group__plan=sum_(rows, "operation_time_group__plan", start=dt.timedelta(0)),
            operation_time_group__fact=sum_(rows, "operation_time_group__fact", start=dt.timedelta(0)),
            idles_sum_group__plan=sum_(rows, "idles_sum_group__plan", start=dt.timedelta(0)),
            idles_sum_group__fact=sum_(rows, "idles_sum_group__fact", start=dt.timedelta(0)),
            undefined_idles_group__fact=sum_(rows, "undefined_idles_group__fact", start=dt.timedelta(0)),
            idles_technical__total_group__plan=sum_(rows, "idles_technical__total_group__plan", start=dt.timedelta(0)),
            idles_technical__total_group__fact=sum_(rows, "idles_technical__total_group__fact", start=dt.timedelta(0)),
            idles_technical__planned_group__plan=sum_(
                rows, "idles_technical__planned_group__plan", start=dt.timedelta(0)
            ),
            idles_technical__planned_group__fact=sum_(
                rows, "idles_technical__planned_group__fact", start=dt.timedelta(0)
            ),
            idles_technical__unplanned_group__fullshift_fact=sum_(
                rows, "idles_technical__unplanned_group__fullshift_fact", start=dt.timedelta(0)
            ),
            idles_technical__unplanned_group__innershift_fact=sum_(
                rows, "idles_technical__unplanned_group__innershift_fact", start=dt.timedelta(0)
            ),
            idles_organization__total_group__plan=sum_(
                rows, "idles_organization__total_group__plan", start=dt.timedelta(0)
            ),
            idles_organization__total_group__fact=sum_(
                rows, "idles_organization__total_group__fact", start=dt.timedelta(0)
            ),
            idles_organization__planned_group__plan=sum_(
                rows, "idles_organization__planned_group__plan", start=dt.timedelta(0)
            ),
            idles_organization__planned_group__fact=sum_(
                rows, "idles_organization__planned_group__fact", start=dt.timedelta(0)
            ),
            idles_organization__unplanned_group__fullshift_fact=sum_(
                rows, "idles_organization__unplanned_group__fullshift_fact", start=dt.timedelta(0)
            ),
            idles_organization__unplanned_group__innershift_fact=sum_(
                rows, "idles_organization__unplanned_group__innershift_fact", start=dt.timedelta(0)
            ),
            ktg__plan=avg(rows, "ktg__plan", ndigits=2),
            ktg__fact=avg(rows, "ktg__fact", ndigits=2),
            kio__plan=avg(rows, "kio__plan", ndigits=2),
            kio__fact=avg(rows, "kio__fact", ndigits=2),
            kio_end_to_end__plan=avg(rows, "kio_end_to_end__plan", ndigits=2),
            kio_end_to_end__fact=avg(rows, "kio_end_to_end__fact", ndigits=2),
            total_trips__plan=sum_(rows, "total_trips__plan"),
            total_trips__fact=sum_(rows, "total_trips__fact"),
            is_total_row=True,
        )
        total_row.vehicle_name = total_name
        return total_row

    def get_rows(self):
        """Получение данных для основных строк"""
        rows = []
        for vehicle_type, vehicle_ids in self.trucks_ids_by_vehicle_type.items():
            vehicle_type_rows = []
            for vehicle_id in vehicle_ids:
                row = self.get_row_for_vehicle(vehicle_id)
                vehicle_type_rows.append(row)
            vehicle_type_rows.append(self.get_total_row(vehicle_type_rows, f"Итого {vehicle_type.name}"))
            rows += vehicle_type_rows
        return rows

    def get_data(self) -> List[SdoGoingOnworkReportRow]:
        """Основной метод возвращает список со строками"""
        return self.rows
