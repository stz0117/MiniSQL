import Catalog
import Index
import time
import Buffer


# import RecordManager


def initialize(path: str):
    Catalog.__initialize__(path)
    Index.__initialize__(path)
    Buffer.__initialize__()


def save():
    Catalog.__finalize__()
    Index.__finalize__()
    Buffer.__finalize__()
    print("All tables have been saved.")


# an example of upcoming args
# S
# [{'attribute_name': 'ID', 'type': 'int', 'type_len': 0, 'unique': True},
# {'attribute_name': 'name', 'type': 'char', 'type_len': 12, 'unique': True},
# {'attribute_name': 'age', 'type': 'int', 'type_len': 0, 'unique': False},
# {'attribute_name': 'gender', 'type': 'char', 'type_len': 1, 'unique': False}]
# ID
def create_table(table_name: str, attributes: list, pk: str):
    time_start = time.time()
    Catalog.exists_table(table_name)
    Index.create_table(table_name)
    Catalog.create_table(table_name, attributes, pk)
    Buffer.create_table(table_name)
    time_end = time.time()
    print("Successfully create table '%s', time elapsed : %fs." %
          (table_name, time_end - time_start))


# e.g. name_index S name
# FAKE!
def create_index(index_name: str, table_name: str, indexed_attr: str):
    Catalog.exists_index(index_name)
    Catalog.create_index(index_name, table_name, indexed_attr)
    Index.create_index(index_name, table_name, indexed_attr)


def drop_table(table_name: str):
    time_start = time.time()
    Catalog.not_exists_table(table_name)
    Catalog.drop_table(table_name)
    Buffer.drop_table(table_name)
    Index.delete_table(table_name)
    time_end = time.time()
    print("Successfully drop table '%s', time elapsed : %fs." %
          (table_name, time_end - time_start))


def drop_index(index_name: str):
    Catalog.not_exists_index(index_name)
    Catalog.drop_index(index_name)


# e.g.
# student
# ['*']
# [{'operator': '>', 'l_op': 'sage', 'r_op': 20},
# {'operator': '=', 'l_op': 'sgender', 'r_op': 'F'},
# {'operator': '<', 'l_op': 'sscore', 'r_op': 89.5}]
def select(table_name: str, attributes: list, where: list = None):
    time_start = time.time()
    Catalog.not_exists_table(table_name)
    Catalog.check_select_statement(table_name, attributes, where)
    #Index.select_from_table(table_name, attributes, where)
    col_dic = Catalog.get_column_dic(table_name)
    print(col_dic)
    results = Buffer.find_record(table_name, col_dic, where)
    print(results)
    numlist = []
    if attributes == ['*']:
        attributes = list(col_dic.keys())
        numlist = list(col_dic.values())
    else:
        for att in attributes:
            print(att)
            numlist.append(col_dic[att])

    print_select(attributes, numlist, results)
    time_end = time.time()
    print(" time elapsed : %fs." % (time_end - time_start))


def print_select(columns_list, columns_list_num, results):
    print('-' * (17 * len(columns_list_num) + 1))
    for i in columns_list:
        if len(str(i)) > 14:
            output = str(i)[0:14]
        else:
            output = str(i)
        print('|', output.center(15), end='')
    print('|')
    print('-' * (17 * len(columns_list_num) + 1))
    for i in results:
        for j in columns_list_num:
            if len(str(i[j])) > 14:
                output = str(i[j])[0:14]
            else:
                output = str(i[j])
            print('|', output.center(15), end='')
        print('|')
    print('-' * (17 * len(columns_list_num) + 1))
    print("Returned %d entries," % len(results), end='')


# e.g. student ['12345678', 'wy', 22, 'M']
def insert(table_name: str, values: list):
    time_start = time.time()
    Catalog.not_exists_table(table_name)
    Catalog.check_types_of_table(table_name, values)
    linenum =Buffer.insert_record(table_name, values)
    Index.insert_into_table(table_name, values,linenum)
    time_end = time.time()
    print(" time elapsed : %fs." % (time_end - time_start))


# e.g. student [{'operator': '=', 'l_op': 'sno', 'r_op': '88888888'}]
def delete(table_name: str, where: list = None):
    time_start = time.time()
    Catalog.not_exists_table(table_name)
    Catalog.check_select_statement(table_name, ['*'], where)  # 从insert中借用的方法
    col = Catalog.get_column_dic(table_name)
    pklist=Buffer.delete_record(table_name, col, where)
    Index.delete_from_table(table_name, pklist)
    time_end = time.time()
    print(" time elapsed : %fs." % (time_end - time_start))


def show_table(table_name: str):
    pass
    # Catalog.exist_table(table_name)
    # Catalog.show_table(table_name) # 显示属性名、类型、大小（char）、是否unique、是否主键、index情况等等


def show_tables():
    pass
    # Catalog.show_tables() # 列出所有表名
