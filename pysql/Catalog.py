import json
import os
import Index

tables = {}#empty dict,to store tables
catalogpath = ''#path of catalogs folder
tablecatalog = ''#path of table catalog file
indexcatalog = ''#path of index catalog file
indices = {}#empty dict,to store indices


class Table():#data structure to save a table
    def __init__(self, table_name, pk = 0):
        self.table_name = table_name
        self.primary_key = pk

    columns = []


class Column():#data structure to save an attribute
    def __init__(self, column_name, is_unique, type='char', length=16):
        self.column_name = column_name
        self.is_unique = is_unique
        self.type = type
        self.length = length


def __initialize__(__path):#initialize the file of catalog
    global catalogpath
    global tablecatalog
    global indexcatalog
    catalogpath = os.path.join(__path, 'dbfiles/catalogs')
    tablecatalog=os.path.join(catalogpath, 'catalog_table')
    indexcatalog=os.path.join(catalogpath, 'catalog_index')

    if not os.path.exists(catalogpath):
        os.makedirs(catalogpath)

        f1 = open(tablecatalog, 'w')
        f2 = open(indexcatalog, 'w')
        f1.close()
        f2.close()

        tables['sys'] = Table('sys', 0)
        indices['sys_default_index'] = {'table': 'sys', 'column': 'attribute1'}
        columns = []
        columns.append(Column('attribute1', True))
        columns.append(Column('attribute2', False))
        tables['sys'].columns = columns
        __savefile__()
    __loadfile__()


def __finalize__():
    __savefile__()



# done
def create_table(table_name, attributes, pk):
    global tables
    cur_table = Table(table_name, pk)
    columns = []
    for attr in attributes:
        columns.append(Column(attr['attribute_name'],
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
        raise Exception("primary_key '%s' does not exist."
                        % cur_table.primary_key)

    tables[table_name] = cur_table


# done
def drop_table(table_name):
    tables.pop(table_name)



# done
def check_types_of_table(table_name, values):
    cur_table = tables[table_name]
    if len(cur_table.columns) != len(values):
        raise Exception("Table '%s' "
                        "has %d columns." % (table_name, len(cur_table.columns)))
    for i, element in enumerate(cur_table.columns):
        if element.type == 'float':
            value = float(values[i])
        elif element.type == 'int':
            value = int(values[i])
        else:
            value = values[i]
            if len(value) > element.length:
                raise Exception("Table '%s' : column '%s' 's length"
                                " can be no longer than %d." % (table_name, element.column_name, element.length))

        if element.is_unique:
            Index.check_unique(table_name, i, value)


# done
def exists_table(table_name):
    for key in tables.keys():
        if key == table_name:
            raise Exception("Table already exists.")


# done
def not_exists_table(table_name):
    for key in tables.keys():
        if key == table_name:
            return
    raise Exception("Table does not exist.")


# done
def not_exists_index(index_name):
    for key in indices.keys():
        if key == index_name:
            return
    raise Exception("Index does not exist.")


# done
def exists_index(index_name):
    for key in indices.keys():
        if key == index_name:
            raise Exception("Index already exists.")


# done
def drop_index(index_name):
    indices.pop(index_name)
    print("Successfully deleted index '%s'." % index_name)


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
                raise Exception("No column"
                                " name '%s'." % i['l_op'])
    if attributes == ['*']:
        return

    for i in attributes:
        if i not in columns:
            raise Exception("No column name '%s'." % i)



def __loadfile__():#from file to memory
    f = open(tablecatalog)
    json_tables = json.loads(f.read())
    for table in json_tables.items():
        temp_name=table[0]
        temp_pk=table[1]['pk']
        temp_columns = []

        __table = Table(temp_name, temp_pk)#table_name&primary key
        for __column in table[1]['columns'].items():
            temp_attname = __column[0]
            temp_isunique = __column[1][0]
            temp_type = __column[1][1]
            temp_len = __column[1][2]

            temp_columns.append(Column(temp_attname,temp_isunique,temp_type,temp_len))
        __table.columns = temp_columns

        tables[temp_name] = __table#add into the tables dict in memory
    f.close()

    f = open(indexcatalog)
    json_indices = f.read()
    json_indices = json.loads(json_indices)
    for index in json_indices.items():
        temp_indexname=index[0]#name of this index
        temp_index=index[1]#the actual component of this index
        indices[temp_indexname] = temp_index
    f.close()


def __savefile__():#from memory to file
    __tables = {}
    for items in tables.items():
        definition = {}
        temp_name=items[0]
        __columns = {}
        for i in items[1].columns:
            __columns[i.column_name] = [i.is_unique, i.type, i.length]

        definition['columns'] = __columns
        definition['pk'] = items[1].primary_key
        __tables[temp_name] = definition

    j_tables = json.dumps(__tables)
    j_indices = json.dumps(indices)

    f = open(tablecatalog, 'w')
    f.write(j_tables)
    f.close()
    f = open(indexcatalog, 'w')
    f.write(j_indices)
    f.close()

