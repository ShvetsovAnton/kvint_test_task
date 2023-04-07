from collections import defaultdict
from pathlib import Path
import pandas


def take_calls_description(calls_statuses_file):
    calls_description = defaultdict(list)
    status_rows = []
    logs = pandas.read_table(calls_statuses_file, sep=';').fillna(
        '', inplace=False
    )
    unique_call_statuses = logs.last_status.unique()
    for call_status in unique_call_statuses:
        status_rows.append(
            logs[logs.last_status == call_status]
        )
    for rows_indexes in status_rows:
        for row_index in rows_indexes.index:
            status = rows_indexes.loc[[row_index]].status[row_index]
            last_status = rows_indexes.loc[[row_index]].last_status[row_index]
            session_id = rows_indexes.loc[[row_index]].session_id[row_index]
            calls_description[status].append(
                {
                    'session_id': session_id,
                    'last_status': last_status
                }
            )
    return calls_description


def read_logs(logs_file):
    with open(logs_file) as file:
        logs_description = file.readlines()
    return logs_description


def find_errors_in_calls(logs, calls_description):
    calls_with_errors = defaultdict(list)
    errors_description = defaultdict(list)
    for errors_session in calls_description:
        if 'delay' in errors_session:
            for session_id in calls_description[errors_session]:
                for log in logs:
                    if session_id['session_id'] in log:
                        calls_with_errors[session_id['session_id']].append(log)
    for session_id in calls_with_errors:
        for log_info in calls_with_errors[session_id]:
            if 'WARNING' in log_info:
                errors_description[session_id].append(log_info)
    return errors_description


def write_errors_description_to_excel(errors_description):
    table_marks = pandas.DataFrame(errors_description.values(),
                                   errors_description.keys())
    writer = pandas.ExcelWriter('errors_description.xlsx')
    table_marks.to_excel(writer)
    writer._save()


def write_total_count_by_calls_to_excel(calls_description):
    calls_endpoint = defaultdict(list)
    total_calls_by_endpoint = {}
    for status in calls_description:
        for status_name in calls_description[status]:
            calls_endpoint[status_name['last_status']].append(status)
    for end_point in calls_endpoint:
        total_calls_by_endpoint[end_point] = len(calls_endpoint[end_point])

    table_marks = pandas.DataFrame(total_calls_by_endpoint.values(),
                                   total_calls_by_endpoint.keys())
    writer = pandas.ExcelWriter('total_calls_by_endpoint.xlsx')
    table_marks.to_excel(writer)
    writer._save()


def main():
    calls_statuses_file = Path.cwd() / 'table.csv'
    logs_file = Path.cwd() / 'all.log'
    logs = read_logs(logs_file)
    calls_description = take_calls_description(calls_statuses_file)
    errors_description = find_errors_in_calls(logs, calls_description)
    write_errors_description_to_excel(errors_description)
    write_total_count_by_calls_to_excel(calls_description)


if __name__ == '__main__':
    main()
