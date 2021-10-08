import multiprocessing
import os
import shutil

from bs4 import BeautifulSoup
import verify_columns

GLOBAL_FIELD_TYPES = []
TABLE_LIST = []
OUTPUT_PATH = '/Volumes/JohnMyPassport/data_repository/VizieR_data/docs/py/'


def parse(file_path):
    print(f'Outpath {OUTPUT_PATH}')
    print(f'Filepath {file_path}')

    html_files = os.listdir(file_path)
    for filename in html_files:
        if filename.startswith('.'):
            continue

        path_name = '/'.join([file_path, filename])
        with open(path_name, 'r') as open_file:
            html = open_file.read()

        soup = BeautifulSoup(html, 'html.parser')
        sections = soup.find_all('section')
        if len(sections) > 1:
            continue

        html_section = sections[0]
        class_attr = html_section.get('class')
        if "ltx_subsection" not in class_attr:
            continue

        # print(f"Processing {filename}")

        # get table name
        page_title = html_section.find_all('h1')[0].text.strip()
        table_name = _get_table_name(page_title)
        # print(f"Table name: {table_name}")
        table_fields = []

        # ugly guts
        # field name is buried in p > span. the field type is in braces: (long).
        # gets the divs then the paragraphs then the spans with in the paragraphs.
        divs = html_section.find_all('div', 'ltx_para')
        for d in divs:
            para = d.find_all('p')
            for p in para:
                if 'div' not in p.parent.name:
                    continue

                spans = p.find_all('span')
                if len(spans) == 0:
                    continue

                if '.' in spans[0].text:
                    continue

                if 'ltx_font_smallcaps' not in spans[0].get('class'):
                    continue

                field_name = spans[0].text.strip()
                if ':' in field_name or 'ltx_ref_tag' in spans[0].get('class'):
                    continue

                type_parts = p.text.split("(")
                if len(type_parts) < 2:
                    continue

                field_type = _get_type_part(type_parts)

                if field_type not in GLOBAL_FIELD_TYPES:
                    GLOBAL_FIELD_TYPES.append(field_type)

                field = {'field_name': field_name, 'field_type': field_type}
                if _table_field_exists(table_fields, field):
                    continue

                table_fields.append(field)

        # print(table_fields)
        # print(f"{filename}")
        # print()
        _make_table_py_files(table_name, table_fields)
    # print(GLOBAL_FIELD_TYPES)

    _build_dynamic_table_create_insert()
    _build_main_py()
    shutil.copyfile('my_utils.py', OUTPUT_PATH + 'my_utils.py')
    shutil.copyfile('requirements.txt', OUTPUT_PATH + 'requirements.txt')


def _build_dynamic_table_create_insert():
    tables = []
    insert = []
    build = []

    build.append("import sqlalchemy")
    build.append("from sqlalchemy import *")
    build.append("from sqlalchemy.orm import sessionmaker, registry, declarative_base, Session")
    build.append("import my_utils")
    # build.append("import concurrent.futures")
    # build.append("from concurrent.futures import ProcessPoolExecutor")
    cnt = 0
    for table_name in TABLE_LIST:
        build.append(f"from {table_name}_model import {_case_table(table_name)}")
        tables.append(f"\t\t\ttable{cnt} = {_case_table(table_name)}()")
        tables.append(f'\t\t\tif not table_inspect.dialect.has_table(conn, table{cnt}.__tablename__):')
        tables.append(f'\t\t\t\ttable{cnt}.__table__.create(conn)')
        tables.append("")
        # insert.append(f"\t\tp{cnt} = pool.submit(insert_data, db_conn, base_directory, table{cnt})")
        insert.append(f"\tinsert_data( db_conn, base_directory, table{cnt})")
        cnt += 1
    build.append("\n")
    build.append("def build_tables(db_conn, base_directory):")
    build.append("\tc_engine = create_engine(db_conn)")
    build.append("\tmetadata = MetaData()")
    build.append("\ttable_inspect = sqlalchemy.inspect(c_engine)")
    build.append("\ttry:")
    build.append("\t\twith c_engine.connect() as conn:")

    build.append("")
    build.append("\n".join(tables))
    build.append("")
    build.append("\t\tmetadata.create_all(c_engine)")
    build.append("\texcept Exception as e:")
    build.append('\t\tprint(f"{e}")')
    build.append('\t\traise Exception(f"{e}")')
    build.append('')
    # build.append("\twith concurrent.futures.ProcessPoolExecutor(max_workers=4) as pool:")
    build.append("\n".join(insert))
    build.append("\n")

    build.append("""def insert_data(db_conn, base_directory, table_obj):

	c_engine = sqlalchemy.create_engine(db_conn)
	with c_engine.connect() as conn:
		table_name = table_obj.__tablename__
		print(f"Inserting to {table_name}")
		csv_file_list = my_utils.get_csv_files(table_name, base_directory)
		if len(csv_file_list) < 1:
			return
		for csv_file in csv_file_list:
			print(f"Working file {csv_file}")
			cnt = 0
			with open(csv_file) as open_file:
				fields = {}
				while True:
					line = open_file.readline()

					if not line:
						break

					# print(f"Line {line}")

					if cnt == 0:
						parts = line.strip().split(',')
						for f in parts:
							if f == 'id':
								f = table_name + "_id"
							fields[f] = f

					if cnt > 0:
						csv = line.strip().split(',')
						insert_field = _build_insert_field(fields, csv)
						updated_field = _parse_insert_field(insert_field, table_obj)

						ins = table_obj.__table__.insert()
						conn.execute(ins, updated_field)

					cnt += 1


def _build_insert_field(field, data):
	cnt = 0
	for key in field:
		field[key] = data[cnt]
		cnt += 1
	# print(field)
	return field


def _parse_insert_field(field, table_obj):
	cols = table_obj.__table__.columns._collection

	for key in field.keys():

		for col in cols:
			if key == col[0]:
				field_type = str(col[1].type).lower()
				if 'int' in field_type:
					field[key] = my_utils.verify_int(field[key])
				if 'float' in field_type:
					field[key] = my_utils.verify_float(field[key])
				if 'bool' in field_type:
					field[key] = my_utils.verify_bool(field[key])
	return field""")

    build.append("\n")
    build.append("def run(db_conn, base_directory):")
    build.append("\tbuild_tables(db_conn, base_directory)")

    out_file = "\n".join(build)
    print(out_file)

    file_path = OUTPUT_PATH + "table_insert.py"
    with open(file_path, 'w') as save_file:
        save_file.write(out_file)


def _get_type_part(type_parts):
    for part in type_parts:
        if _is_accepted_type(part):
            return part


def _get_table_name(page_title):
    table = page_title.split(' ')
    if len(table) > 0:
        name = table[1]
        if name.strip() == 'tmass_psc_xsc_best_neighbour':
            return 'tmasspscxsc_best_neighbour'
        if name.strip() == 'tmass_psc_xsc_join':
            return 'tmasspscxsc_join'
        if name.strip() == 'tmass_psc_xsc_neighbourhood':
            return 'tmasspscxsc_neighbourhood'
        return name


def _parse_type(field_type):
    parsed = field_type.split(',')
    for p in parsed:
        if _is_accepted_type(p):
            return p

    return field_type


def _table_field_exists(table_fields, field_dict):
    value = field_dict['field_name']
    for field in table_fields:
        if field['field_name'] == value:
            return True

    return False


def _make_table_py_files(table_name, table_fields):
    is_valid = verify_columns.run(table_name, table_fields)
    if not is_valid:
        return

    TABLE_LIST.append(table_name)
    _build_table_class(table_name, table_fields)
    # commenting out to make room for something a little more dynamic.
    # _build_table_py_create(table_name, table_fields)


def _build_table_py_create(table_name, table_fields):
    output_name = f"{OUTPUT_PATH}/{table_name}_data_import.py"

    print(f'Building python insert-create file for {table_name}')

    output = '/Volumes/JohnMyPassport/data_repository/VizieR_data/docs/py/' + table_name + '_insert.py'
    build = []

    build.append("import sqlalchemy")
    build.append("from sqlalchemy import *")
    build.append("from sqlalchemy.orm import sessionmaker, registry, declarative_base, Session")
    build.append("import my_utils")
    build.append(f"from {table_name}_model import {_case_table(table_name)}")
    build.append("")

    build.append("def create_table(db_conn):")
    build.append("\ttry:")
    build.append("\t\tc_engine = create_engine(db_conn)")
    build.append("\t\tmetadata = MetaData()")
    build.append(f"\t\tmake_table = Table('{table_name}',")
    build.append("\t\t\t\t\t\tmetadata,")
    build.append(f"\t\t\t\t\t\tColumn('id', BigInteger, primary_key=True),")
    build_values = []
    for field in table_fields:
        field_name = field['field_name']
        if field_name == 'id':
            field_name = table_name + "_id"
        build_values.append(f"\t\t\t\t\t\tColumn('{field_name}', {_field_type_mappings(field['field_type'])})")
    build.append(",\n".join(build_values))
    build.append("\t\t\t\t\t\t)")

    build.append('\t\tmetadata.create_all(c_engine)')
    build.append('\texcept Exception as e:')
    build.append('\t\tprint(f"{e}")')
    build.append('\t\traise Exception(f"{e}")')
    build.append('\n')

    # start insert_data

    build.append('def insert_data(db_conn, csv_files, is_test):')
    build.append(f"\tprint('Starting insert for {table_name}')")
    build.append("\tprint(f'is test: {is_test}')")
    build.append("\tprint(f'Number of files: {len(csv_files)}')")
    build.append("")
    build.append('\tif csv_files is None or len(csv_files) < 1:')
    build.append('\t\treturn')
    build.append('')
    build.append("\tengine = ''")
    build.append("\tsession = ''")
    build.append('')
    build.append('\tif not is_test:')
    build.append("\t\tengine = sqlalchemy.create_engine(db_conn)")
    build.append("\t\tsession = Session(engine)")
    build.append("")
    build.append("\tfor csv in csv_files:")
    build.append("\t\tcnt = -1")
    build.append("\t\tprint(f'processing: {csv}')")
    build.append("\t\twith open(csv, 'r') as open_file:")
    build.append("\t\t\twhile True:")
    build.append("\t\t\t\tcsv_line = open_file.readline()")
    build.append("\t\t\t\tif not csv_line:")
    build.append("\t\t\t\t\tbreak")
    build.append("\t\t\t\tcnt += 1")
    build.append("\t\t\t\tif cnt == 0:")
    build.append("\t\t\t\t\tcontinue")
    build.append('\t\t\t\tcsv_values = csv_line.split(",")')

    build.append("\t\t\t\ttry:")
    build.append(f"\t\t\t\t\ttable_insert = {_case_table(table_name)}(")
    build_values = []
    cnt = 0
    for field in table_fields:
        field_type = _field_type_mappings(field['field_type'])
        field_name = field['field_name']
        if field_name == 'id':
            field_name = table_name + "_id"

        field_value = f'csv_values[{cnt}].strip()'
        if field_type == 'BigInteger' or field_type == 'Integer':
            field_value = f'my_utils.verify_int(csv_values[{cnt}].strip())'
        if field_type == 'Float':
            field_value = f'my_utils.verify_float(csv_values[{cnt}].strip())'
        if field_type == 'Boolean':
            field_value = f'my_utils.verify_bool(csv_values[{cnt}].strip())'

        build_values.append(f"\t\t\t\t\t\t{field_name}={field_value}")
        cnt += 1
    build.append(",\n".join(build_values))
    build.append("\t\t\t\t\t)")
    build.append('\t\t\t\t\tif is_test:')
    build.append('\t\t\t\t\t\tprint(f"Test: {csv}")')
    build.append('\t\t\t\t\t\tprint(f"Test: {csv_line.strip()}")')
    build.append('\t\t\t\t\telse:')
    build.append("\t\t\t\t\t\tsession.add(table_insert)")
    build.append("\t\t\t\t\t\tsession.flush()")
    build.append("\t\t\t\t\t\tsession.commit()")
    build.append("\t\t\t\texcept Exception as e:")
    build.append("\t\t\t\t\tprint(f'{e}')")
    build.append("\t\t\t\t\traise Exception(f'{e}')")

    build.append("\n")
    build.append("def run(db_conn, base_directory, is_test):")
    build.append("\tcreate_table(db_conn)")
    build.append(f"\tcsv_files = my_utils.get_csv_files('{table_name}', base_directory)")

    build.append(f"\tprint('{table_name} has csv files.')")
    build.append("\tprint(len(csv_files))")
    build.append(f"\tinsert_data(db_conn, csv_files, is_test)")

    build_out = "\n".join(build)
    print(build_out)
    print()
    with open(output_name, 'w') as out_file:
        out_file.write(build_out)


def _build_table_class(table_name, table_fields):
    print(f'Building python file for {table_name}')

    output = OUTPUT_PATH + table_name + '_model.py'
    build = []
    repr_values = []
    repr_self = []

    build.append("""from sqlalchemy import Column, Integer, String, Float, BigInteger, Boolean, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()
""")

    build.append(f"class {_case_table(table_name)}(Base):")
    build.append(f"\t__tablename__ = '{table_name}'")
    build.append('\tid = Column(Integer, primary_key=True)')

    for field in table_fields:
        field_name = field['field_name']
        if field_name == 'id':
            field_name = table_name + '_id'
        build.append(f"\t{field_name} = Column({_field_type_mappings(field['field_type'])})")
        repr_values.append(f"{field_name}='{{self.{field_name}}}'")
        repr_self.append(f"self.{field_name}")

    build.append("\tdef __repr__(self):")
    build.append(f"\t\treturn \"<{_case_table(table_name)}({', '.join(repr_values)})>\"")

    result = "\n".join(build)
    # print(result)

    with open(output, 'w') as out_file:
        out_file.write(result)


def _field_type_mappings(field_type):
    if 'string' in field_type:
        return 'Text'
    if 'long' in field_type:
        return 'BigInteger'
    if 'double' in field_type:
        return 'Float'
    if 'float' in field_type:
        return 'Float'
    if 'int' in field_type:
        return 'Integer'
    if 'boolean' in field_type:
        return 'Boolean'
    if 'short' in field_type:
        return 'Integer'
    if 'byte' in field_type:
        return 'Integer'
    if 'char' in field_type:
        return 'String(1)'
    return field_type


def _is_accepted_type(text):
    if 'string' in text:
        return True
    if 'long' in text:
        return True
    if 'double' in text:
        return True
    if 'float' in text:
        return True
    if 'boolean' in text:
        return True
    if 'short' in text:
        return True
    if 'byte' in text:
        return True
    if 'char' in text:
        return True
    if 'int' in text:
        return True
    return False


def _case_table(table_name):
    names = table_name.replace("_", " ")
    names = str.title(names)
    return names.replace(' ', '_')


def _build_main_py():
    output_file = OUTPUT_PATH + 'main.py'
    build = []
    parsers = []
    procs = []
    cnt = 0
    for t in TABLE_LIST:
        cnt += 1
        # build.append(f"import {t}_data_import")
        # parsers.append(f"\tprint('Starting: {t}')")
        # parsers.append(f"\t{t}_data_import.run(connection, BASE_DIRECTORY, TEST_RUN)")
        # parsers.append(f"\tprint('Finished: {t}')")
        # parsers.append('')
        procs.append(f"\t\tp{cnt} = pool.submit({t}_data_import.run, connection, BASE_DIRECTORY, TEST_RUN)")

    build.append("")
    build.append("import my_utils")
    build.append("import table_insert")
    build.append("")
    build.append("import sys")
    build.append("import platform")
    build.append("import concurrent.futures")
    build.append("from concurrent.futures import ProcessPoolExecutor")


    build.append("")
    build.append('BASE_DIRECTORY = "/Volumes/source/big_data_raw/big_data_raw"')
    build.append('MYSQL = "mysql+pymysql://john:a@datahost.local/big_data_astro_db?charset=utf8mb4"')
    build.append('TESTDB = "mysql+pymysql://john:a@datahost.local/big_data_test?charset=utf8mb4"')
    build.append('PRODUCTION = False')
    build.append('TEST_RUN = True')

    build.append('\n')
    build.append("def run():")
    build.append('\tbase = BASE_DIRECTORY')
    build.append("\tconnection = TESTDB")
    build.append('\tif PRODUCTION:')
    build.append('\t\tconnection = MYSQL')
    build.append("")
    build.append("\thostname = platform.node().lower()")
    build.append("\tif 'john' not in hostname:")
    build.append('\t\tbase = "/mnt/source/big_data_raw/big_data_raw"')
    build.append('\t\tconnection = connection.replace("datahost.local", "localhost")')
    # build.append("\twith concurrent.futures.ProcessPoolExecutor(max_workers=4) as pool:")
    # build.append("\n".join(procs))
    build.append('\ttable_insert.run(connection, base)')

    build.append("\n")
    build.append("if __name__ == '__main__':")
    build.append("\tprint('Starting to insert data.')")
    build.append("\trun()")
    build.append("\tprint('Finished inserting all data.')")
    build.append('')

    output = "\n".join(build)
    print(output)
    with open(output_file, 'w') as out_file:
        out_file.write(output)