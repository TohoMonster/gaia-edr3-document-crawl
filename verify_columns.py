import os

BASE_DIR = '/Volumes/source/big_data_raw/big_data_raw'


def run(table_name, table_fields) -> bool:
    table_directory = _find_table_directory(table_name)
    if table_directory is None:
        print(f'\n{table_name} is not a directory.\n')
        return False

    table_csv = _get_one_table_csv(table_directory)

    if table_csv is None:
        print(f'{table_name} has no CSV')
        return False

    csv_header = _parse_csv_header(table_csv)
    if csv_header is None:
        print(f"{table_name} has no CSV header")
        return False

    is_valid = _verify_fields(table_fields, csv_header)

    # print(f"\n{table_name} is valid: {is_valid}\n")
    return is_valid


def _verify_fields(table_fields, csv_header) -> bool:
    is_in_csv = False
    is_in_fields = False

    for tf in table_fields:
        field_name = tf['field_name']
        for ch in csv_header:
            if field_name == ch:
                is_in_csv = True
                break

    for ch in csv_header:
        for tf in table_fields:
            if ch == tf['field_name']:
                is_in_fields = True
                break

    if is_in_fields and is_in_csv:
        return True

    if not is_in_fields:
        print("Not in fields.")

    if not is_in_csv:
        print('Not in csv.')

    return False


def _parse_csv_header(table_csv) -> list:
    if table_csv is None:
        return None

    return table_csv.strip().split(',')


def _find_table_directory(table_name) -> str:
    dir_list = os.listdir(BASE_DIR)
    for d in dir_list:
        crawl_dir = '/'.join([BASE_DIR, d])

        if crawl_dir.endswith('.txt') or d.startswith('.'):
            continue

        d_list = os.listdir(crawl_dir)
        if table_name in d_list:
            return '/'.join([crawl_dir, table_name])

    return None


def _get_one_table_csv(table_directory) -> str:
    if table_directory is None:
        return None

    cav_list = os.listdir(table_directory)
    for csv in cav_list:
        if csv.endswith('.csv'):
            csv_file = '/'.join([table_directory, csv])
            with open(csv_file, 'r') as csv_open:
                return csv_open.readline()


