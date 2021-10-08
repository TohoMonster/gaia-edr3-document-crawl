import os

def verify_int(value) -> int:
    if value is None or value == '':
        return
    return int(value)


def verify_float(value) -> float:
    if value is None or value == '':
        return
    return float(value)


def verify_bool(value) -> bool:
    if value is None or value == '':
        return
    return bool(value)


def get_table_directory(table_name, base_dir) -> str:
    dir_list = os.listdir(base_dir)
    for d in dir_list:
        crawl_dir = '/'.join([base_dir, d])

        if crawl_dir.endswith('.txt') or d.startswith('.'):
            continue

        if not os.path.isdir(crawl_dir):
            continue

        d_list = os.listdir(crawl_dir)
        if table_name in d_list:
            return '/'.join([crawl_dir, table_name])

    return None


def get_table_csv_list(table_directory) -> list:
    if table_directory is None:
        return None

    csv_files = []
    cav_list = os.listdir(table_directory)
    for csv in cav_list:
        if csv.endswith('.csv'):
            csv_file = '/'.join([table_directory, csv])
            csv_files.append(csv_file)

    return csv_files


def get_csv_files(table_name, base_directory) -> list:
    table_directory = get_table_directory(table_name, base_directory)
    if table_directory is None:
        return None

    csv_list = get_table_csv_list(table_directory)

    if len(csv_list) < 1:
        return None

    return csv_list
