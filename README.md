class EditWagonTripsPackAction(ActionView):
    name = "underground_edit_wagon_trips_pack"
    parent = BaseWagonTripMultiEditView
    post_actions = ["edit_wagon_trips_pack"]
    dashboard_actions = {1: "Пакетное редактирование рейсов ВШТ"}

    @attr.s
    class EditWagonTripsDataModel:
        wagon_trips_ids: List[int] = attr.ib(factory=list, converter=json.loads)
        selected_unload_source_id: int = attr.ib(default=0, converter=int)
        selected_loco_wagon_id: int = attr.ib(default=0, converter=int)
        selected_unload_dest_id: int = attr.ib(default=0, converter=int)
        selected_wagon_count: int = attr.ib(default=0, converter=int)

        @property
        def wagon_trips(self) -> Union[List[WagonTrip], QuerySet]:
            return WagonTrip.objects.filter(pk__in=self.wagon_trips_ids).select_related(
                "loco_wagon", "unload_source", "unload_dest"
            )

        @classmethod
        def get_instance(cls, model, pk) -> Any:
            if pk > 0:
                return model.objects.get(pk=pk)

        def get_new_dict_if_pk_exist(self, field_name_id, pk, updating_dict: Dict[str, int]) -> Dict[str, int]:
            updating_dict_copy = updating_dict.copy()
            if pk > 0:
                updating_dict_copy[field_name_id] = pk
            return updating_dict_copy

        @cached_property
        def loco_wagon(self) -> Union[Wagon, None]:
            model = Wagon
            pk = self.selected_loco_wagon_id
            return self.__class__.get_instance(model, pk)

        @cached_property
        def unload_source(self) -> Union[Unload, None]:
            model = Unload
            pk = self.selected_unload_source_id
            return self.__class__.get_instance(model, pk)

        @cached_property
        def unload_dest(self) -> Union[Unload, None]:
            model = Unload
            pk = self.selected_unload_dest_id
            return self.__class__.get_instance(model, pk)

        @cached_property
        def wagon_count(self):
            model = WagonTrip
            pk = self.selected_wagon_count
            return self.__class__.get_instance(model, pk)

        def get_updating_dict(self) -> Dict[str, int]:
            result = dict()
            result = self.get_new_dict_if_pk_exist("unload_source", self.selected_unload_source_id, result)
            result = self.get_new_dict_if_pk_exist("loco_wagon", self.selected_loco_wagon_id, result)
            result = self.get_new_dict_if_pk_exist("unload_dest", self.selected_unload_dest_id, result)
            result = self.get_new_dict_if_pk_exist("wagon_count", self.selected_wagon_count, result)
            return result

    def get_edit_trips_data_model(self, request) -> "EditWagonTripsDataModel":
        raw_data: Dict[str, List[str]] = request.POST

        def get_values_for_like_key(target_key: str, data: Dict[str, List[str]]) -> List[str]:
            for key, value in data.items():
                if target_key in key:
                    return value
            raise KeyError()

        data_model = self.__class__.EditWagonTripsDataModel(
            wagon_trips_ids=get_values_for_like_key("selectedWagonTrips", raw_data),
            selected_unload_source_id=get_values_for_like_key("selectedUnloadSource", raw_data),
            selected_loco_wagon_id=get_values_for_like_key("selectedLocoWagon", raw_data),
            selected_unload_dest_id=get_values_for_like_key("selectedUnloadDest", raw_data),
            selected_wagon_count=get_values_for_like_key("selectedWagonCount", raw_data),
        )
        return data_model

    def get_trip_old_values(self, wagon_trip: WagonTrip) -> Dict[str, int]:
        result = {}
        if wagon_trip.unload_source_id:
            result["unload_source"] = wagon_trip.unload_source_id
        if wagon_trip.loco_wagon_id:
            result["loco_wagon"] = wagon_trip.loco_wagon_id
        if wagon_trip.unload_dest_id:
            result["unload_dest"] = wagon_trip.unload_dest_id
        if wagon_trip.wagon_count:
            result["wagon_count"] = wagon_trip.wagon_count
        return result

    def edit_wagon_trips_pack(self, request):
        enterprise_id = ENTERPRISE_ID
        form_class = BaseWagonTripMultiEditView.WagonTripEditForm
        result = dict()
        try:
            data_model = self.get_edit_trips_data_model(request)

            def log_if_exist_value(wagon_trip: WagonTrip, field_name: str, value: object) -> "":
                if value:
                    old_value = getattr(wagon_trip, field_name)
                    return (
                        f"Рейс ВШТ ({wagon_trip.id}) с началом разгрузки {wagon_trip.unload_begin_time} "
                        f'с локомотивом {wagon_trip.loco_wagon} --- изменено поле {field_name} с "{old_value}" на "{value}"\n'
                    )
                return ""

            message_to_log = ""
            with transaction.atomic():
                updating_data = data_model.get_updating_dict()
                for wagon_trip in data_model.wagon_trips:
                    message_to_log += log_if_exist_value(
                        wagon_trip,
                        field_name="unload_source",
                        value=data_model.unload_source,
                    )
                    message_to_log += log_if_exist_value(
                        wagon_trip, field_name="loco_wagon", value=data_model.loco_wagon
                    )
                    message_to_log += log_if_exist_value(
                        wagon_trip,
                        field_name="unload_dest",
                        value=data_model.unload_dest,
                    )
                    message_to_log += log_if_exist_value(
                        wagon_trip,
                        field_name="wagon_count",
                        value=data_model.selected_wagon_count,
                    )
                    form_data = self.get_trip_old_values(wagon_trip)
                    form_data.update(updating_data)
                    form = form_class(form_data.copy(), instance=wagon_trip)
                    if form.is_valid():
                        item = WagonTripMultiEditView.update_object_cls(form, commit=False)
        except Exception as exc:
            result["errors"] = exc.__repr__()
        else:
            trip_word_form = word_number_forms(len(data_model.wagon_trips), "рейсов", "рейс", "рейса")
            change_word_form = word_number_forms(len(data_model.wagon_trips), "измененено", "изменен", "изменено")
            result["message"] = f"{len(data_model.wagon_trips)} {trip_word_form} успешно {change_word_form}"
            self.dashboard(
                enterprise=enterprise_id,
                action=1,
                description=message_to_log,
                model="WagonTrip",
                object_id=str(data_model.wagon_trips_ids),
            )
        return self.j(result)
