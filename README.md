        norm_intervals = []
        # Получаем интервалы для отобранных норм
        for idle_norm in norms_filtered:
            for interval in idle_norm.interval_type.all():
                interval: IdleTypeIntervals
                norm_interval = {
                    'time_begin': interval.time_begin,
                    'time_end': interval.time_end,
                    'idle_type': interval.idle_type
                }
                norm_intervals.append(norm_interval)   
        norm_intervals.sort(key=lambda n: n['time_begin'])

        # Выделяем свободные интервалы и пересечение норм
        free_interval_name = "Свободный интервал"

        for index in range(len(norm_intervals) - 1):
            current_norm_interval = norm_intervals[index]
            next_norm_interval = norm_intervals[index + 1]

            current_time_end = current_norm_interval['time_end']
            next_time_begin = next_norm_interval['time_begin']

            if next_time_begin != current_time_end and current_time_end < next_time_begin:
                free_interval = {
                    "time_begin": current_time_end,
                    "idle_type__name": free_interval_name,
                    "time_end": next_time_begin
                    }
                norm_intervals.append(free_interval)

            if current_time_end > next_time_begin:
                current_norm_interval['intersection'] = True
                next_norm_interval['intersection'] = True

            if current_norm_interval["idle_type"] == next_norm_interval["idle_type"]:
                current_norm_interval["time_end"] = next_norm_interval["time_end"]
                norm_intervals.pop(index + 1)

        norm_intervals.sort(key=lambda n: n['time_begin'])
