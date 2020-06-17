import json
import os
import Index

tables = {}
path = ''
indices = {}


class table_instance():
    def __init__(self, table_name, primary_key=0):
        self.table_name = table_name
        self.primary_key = primary_key

    columns = []


class column():
    def __init__(self, column_name, is_unique, type='char', length=16):
        self.column_name = column_name
        self.is_unique = is_unique
        self.type = type
        self.length = length


def __initialize__(__path):
    global path
    path = __path
    if not os.path.exists(os.path.join(path, 'dbfiles/catalog_files')):
        os.makedirs(os.path.join(path, 'dbfiles/catalog_files'))
        f = open(os.path.join(path, 'dbfiles/catalog_files/tables_catalog.msql'), 'w')
        f.close()
        f = open(os.path.join(path, 'dbfiles/catalog_files/indices_catalog.msql'), 'w')
        f.close()
        tables['sys'] = table_instance('sys', 0)
        indices['sys_default_index'] = {'table': 'sys', 'column': 'username'}
        columns = []
        columns.append(column('username', True))
        columns.append(column('password', False))
        tables['sys'].columns = columns
        __store__()
    __load__()


def __finalize__():
    __store__()


def __load__():
    f = open(os.path.join(path, 'dbfiles/catalog_files/tables_catalog.msql'))
    json_tables = json.loads(f.read())
    for table in json_tables.items():
        __table = table_instance(table[0], table[1]['primary_key'])
        columns = []
        for __column in table[1]['columns'].items():
            columns.append(column(__column[0],
                                  __column[1][0], __column[1][1], __column[1][2]))
        __table.columns = columns
        tables[table[0]] = __table
    f.close()
    f = open(os.path.join(path, 'dbfiles/catalog_files/indices_catalog.msql'))
    json_indices = f.read()
    json_indices = json.loads(json_indices)
    for index in json_indices.items():
        indices[index[0]] = index[1]
    f.close()


def __store__():
    __tables = {}
    for items in tables.items():
        definition = {}
        definition['primary_key'] = items[1].primary_key
        __columns = {}
        for i in items[1].columns:
            __columns[i.column_name] = [i.is_unique, i.type, i.length]
        definition['columns'] = __columns
        __tables[items[0]] = definition
    json_tables = json.dumps(__tables)
    f = open(os.path.join(path, 'dbfiles/catalog_files/tables_catalog.msql'), 'w')
    f.write(json_tables)
    f.close()
    f = open(os.path.join(path, 'dbfiles/catalog_files/indices_catalog.msql'), 'w')
    f.write(json.dumps(indices))
    f.close()


# done
def check_types_of_table(table_name, values):
    cur_table = tables[table_name]
    if len(cur_table.columns) != len(values):
        raise Exception("Catalog Module : table '%s' "
                        "has %d columns." % (table_name, len(cur_table.columns)))
    for index, i in enumerate(cur_table.columns):
        if i.type == 'int':
            value = int(values[index])
        elif i.type == 'float':
            value = float(values[index])
        else:
            value = values[index]
            if len(value) > i.length:
                raise Exception("Catalog Module : table '%s' : column '%s' 's length"
                                " can't be longer than %d." % (table_name, i.column_name, i.length))

        if i.is_unique:
            Index.check_unique(table_name, index, value)


# done
def exists_table(table_name):
    for key in tables.keys():
        if key == table_name:
            raise Exception("Catalog Module : table already exists.")


# done
def not_exists_table(table_name):
    for key in tables.keys():
        if key == table_name:
            return
    raise Exception("Catalog Module : table not exists.")


# done
def not_exists_index(index_name):
    for key in indices.keys():
        if key == index_name:
            return
    raise Exception("Catalog Module : index not exists.")


# done
def exists_index(index_name):
    for key in indices.keys():
        if key == index_name:
            raise Exception("Catalog Module : index already exists.")


# done
def create_table(table_name, attributes, pk):
    global tables
    cur_table = table_instance(table_name, pk)
    columns = []
    for attr in attributes:
        columns.append(column(attr['attribute_name'],
                              attr['unique'],
                              attr['type'],
                              attr['type_len']))

    cur_table.columns = columns
    seed = False
    for index, __column in enumerate(cur_table.columns):
        if __column.column_name == cur_table.primary_key:
            cur_table.primary_key = index
            seed = True
            break
    if seed == False:
        raise Exception("Catalog Module : primary_key '%s' not exists."
                        % cur_table.primary_key)

    tables[table_name] = cur_table


# done
def drop_table(table_name):
    tables.pop(table_name)


# done
def drop_index(index_name):
    indices.pop(index_name)
    print("Successfully delete index '%s'." % index_name)


# done
def create_index(index_name, table, column):
    indices[index_name] = {'table': table, 'column': column}


# done
def check_select_statement(table_name, attributes, where):
    # raise an exception if something is wrong
    columns = []
    for i in tables[table_name].columns:
        columns.append(i.column_name)
    if where is not None:
        for i in where:
            if i['l_op'] not in columns:
                raise Exception("Catalog Module : no such column"
                                " name '%s'." % i['l_op'])
    if attributes == ['*']:
        return

    for i in attributes:
        if i not in columns:
            raise Exception("Catalog Module : no such column name '%s'." % i)


if __name__ == '__main__':
    # new_table = table_instance('my_table','yes')
    # new_table.columns.append(column('yes',True))
    # new_table.columns.append(column('no',False))
    # tables['my_table'] = new_table
    __initialize__('/Users/alan/Desktop/CodingLife/Python/miniSQL')
    ##__store__()
