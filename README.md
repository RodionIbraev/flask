index = 0
while index < len(norm_intervals) - 1:
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
        norm_intervals.sort(key=lambda n: n['time_begin'])
        # Пересчитываем индекс в случае добавления нового элемента
        index += 1

    if current_time_end > next_time_begin:
        current_norm_interval['intersection'] = True
        next_norm_interval['intersection'] = True

    if current_norm_interval["idle_type"] == next_norm_interval["idle_type"]:
        current_norm_interval["time_end"] = next_norm_interval["time_end"]
        norm_intervals.pop(index + 1)
    else:
        index += 1
