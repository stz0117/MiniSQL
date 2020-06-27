import Catalog
import math
import json
import os

N = 4

tables = {}
recordpath = ''
__last_leaf_pointer = ''


class node():
    def __init__(self, isleaf, line0, keys0, pointers0, parent0=''):
        self.is_leaf = isleaf
        self.line = line0
        self.keys = keys0
        self.pointers = pointers0
        self.parent = parent0


def __initialize__(__path):
    global recordpath
    recordpath = os.path.join(__path, 'dbfiles/indices')

    if not os.path.exists(recordpath):
        os.makedirs(recordpath)
        tables['sys'] = node(True, [], ['key0'], [['key0', 'key1'], ''])
        __store__()

    __load__()


def __finalize__():
    __store__()

#def __init__(self, isleaf, keys0, pointers0, parent0=''):
def __load__():
    global __last_leaf_pointer
    print(recordpath)
    f = open(os.path.join(recordpath, 'indexfile'))
    json_tables = json.loads(f.read())
    f.close()
    for table in json_tables.items():
        temp_name = table[0]
        temp_content = table[1]
        if len(temp_content['keys']) == 0:
            tables[temp_name] = node(True, 0, [], [])
            continue
        tables[temp_name] = \
            node(temp_content['is_leaf'],temp_content['line'],temp_content['keys'], temp_content['pointers'], '')
        if tables[temp_name].is_leaf:
            continue

        tables[temp_name].pointers = \
            load_nodes(temp_content['pointers'], tables[temp_name])


def load_nodes(pointer_list, parent):
    global __last_leaf_pointer
    nodelist = []
    for pointer in pointer_list:

        if pointer['is_leaf']:
            new_node = node(pointer['is_leaf'], pointer['line'],pointer['keys'], pointer['pointers'], parent)
            nodelist.append(new_node)
            if __last_leaf_pointer == '':
                __last_leaf_pointer = new_node
            else:
                __last_leaf_pointer.pointers.append(new_node)
                __last_leaf_pointer = new_node
        else:
            new_node = node(pointer['is_leaf'], pointer['line'],pointer['keys'], pointer['pointers'], parent)
            nodelist.append(new_node)
            new_node.pointers = load_nodes(pointer['pointers'], nodelist[-1])
    return nodelist

def __store__():
    global recordpath
    __tables = {}
    for table in tables.items():
        __tables[table[0]] = recursive_store_node(table[1])

    f = open(os.path.join(recordpath, 'indexfile'), 'w')
    json_tables = json.dumps(__tables)
    f.write(json_tables)
    f.close()


def recursive_store_node(node):
    cur_node = {}
    cur_node['is_leaf'] = node.is_leaf
    cur_node['line'] = node.line
    cur_node['keys'] = node.keys
    cur_node['line'] = node.line
    if node.is_leaf == True and node.pointers == []:
        pass
    elif node.is_leaf == True and node.pointers[-1] != '':
        cur_node['pointers'] = node.pointers[0:-1]
    elif node.is_leaf == True and node.pointers[-1] == '':
        cur_node['pointers'] = node.pointers
    else:
        cur_node['pointers'] = []
        for __node in node.pointers:
            cur_node['pointers'].append(recursive_store_node(__node))
    return cur_node

'''
def __prints(table):
    node = tables[table]
    __do_print(node)


def __do_print(node):
    if node.is_leaf == True:
        print('leaf')
        print(node.keys)
        print(node.pointers)
        if node.parent != '':
            print('parent:', node.parent.keys)
    else:
        print('node')
        print(node.keys)
        if node.parent != '':
            print('parent:', node.parent.keys)
        for i in node.pointers:
            __do_print(i)
'''

# done
def insert_into_table(table_name, __values,line_number:int):
    '''
    for i, col in enumerate(Catalog.tables[table_name].columns):
        if col.type == 'int':
            __values[i] = int(__values[i])
        elif col.type == 'char':
            __values[i] = str(__values[i])
        elif col.type == 'float':
            __values[i] = float(__values[i])'''

    cur_node = tables[table_name]
    __primary_key = Catalog.tables[table_name].primary_key
    # __primary_key = 0
    if len(cur_node.keys) == 0:
        # new tree
        cur_node.keys.append(__values[__primary_key])
        cur_node.pointers.append([__values[__primary_key]]+[line_number])
        cur_node.pointers.append('')
        print('Successfully insert into table %s,' % table_name, end='')
        return

    cur_node = find_leaf_place(table_name, __values[__primary_key])
    if len(cur_node.keys) < N - 1:
        insert_into_leaf(cur_node, __values[__primary_key], [__values[__primary_key]]+[line_number])

    else:
        insert_into_leaf(cur_node, __values[__primary_key], [__values[__primary_key]]+[line_number])
        new_node = node(True, [],[], [])
        tmp_keys = cur_node.keys
        tmp_pointers = cur_node.pointers
        cur_node.keys = []
        cur_node.pointers = []
        for i in range(math.ceil(N / 2)):
            cur_node.keys.append(tmp_keys.pop(0))
            cur_node.pointers.append(tmp_pointers.pop(0))
        for i in range(N - math.ceil(N / 2)):
            new_node.keys.append(tmp_keys.pop(0))
            new_node.pointers.append(tmp_pointers.pop(0))
        cur_node.pointers.append(new_node)
        new_node.pointers.append(tmp_pointers.pop(0))
        insert_into_parent(table_name, cur_node, new_node.keys[0], new_node)

    print('Successfully insert into table %s,' % table_name, end='')


# done
def create_table(table_name):
    tables[table_name] = node(True,[], [], [])


# done
def delete_table(table_name):
    tables.pop(table_name)


# done
def delete_from_table(table_name, where):
    # delete rows from table according to the statement's condition
    # usage : find_leaf_place_with_condition(table, column, condition, value)
    if where is None:
        tables[table_name] = node(True, [],[], [])
        print("Successfully delete all entrys from table '%s'," % table_name, end='')
    else:
        columns = {}
        for index, col in enumerate(Catalog.tables[table_name].columns):
            columns[col.column_name] = index
        __primary_key = Catalog.tables[table_name].primary_key
        # __primary_key = 0
        # columns = {'num':0,'val':1}

        times = 0
        while True:
            nodes = find_leaf_place_with_condition(table_name, columns[where[0]['l_op']], where[0]['operator'], where[0]['r_op'])
            for cond in where:
                if columns[cond['l_op']] == __primary_key:
                    nodes = find_leaf_place_with_condition(table_name, columns[cond['l_op']], cond['operator'], cond['r_op'])
                    break

            if len(nodes) == 0:
                break
            seed = False
            for __node in nodes:
                if seed == True:
                    break
                for index, leaf in enumerate(__node.pointers[0:-1]):
                    if check_conditions(leaf, columns, where):
                        __node.pointers.pop(index)
                        __node.keys.pop(index)
                        maintain_B_plus_tree_after_delete(table_name, __node)
                        times = times + 1
                        seed = True
                        break
            if seed == False:
                break
        print("Successfully delete %d entry(s) from table '%s'," % (times, table_name), end='')


# done
op_list = ['<','<=','>','>=','<>','=']

def check_conditions(leaf, columns, where):
    for cond in where:
        # cond <-> column op value
        __value = leaf[columns[cond['l_op']]]
        if cond['operator'] not in op_list:
            raise Exception("Index Module : unsupported op.")
        else:
            if cond['operator'] == op_list[0]:
                if not (__value < cond['r_op']):
                    return False
            elif cond['operator'] == op_list[1]:
                if not (__value <= cond['r_op']):
                    return False
            elif cond['operator'] == op_list[2]:
                if not (__value > cond['r_op']):
                    return False
            elif cond['operator'] == op_list[3]:
                if not (__value >= cond['r_op']):
                    return False
            elif cond['operator'] == op_list[4]:
                if not (__value != cond['r_op']):
                    return False
            elif cond['operator'] == op_list[5]:
                if not (__value == cond['r_op']):
                    return False
        return True


# done
def maintain_B_plus_tree_after_delete(table, __node):
    global N
    if __node.parent == '' and len(__node.pointers) == 1:
        __node.pointers = []
    elif ((len(__node.pointers) < math.ceil(N / 2) and __node.is_leaf == False) or
          (len(__node.keys) < math.ceil((N - 1) / 2) and __node.is_leaf == True)) \
            and __node.parent != '':
        previous = False
        other_node = node(True, [], [])
        K = ''
        __index = 0
        for index, i in enumerate(__node.parent.pointers):
            if i == __node:
                if index == len(__node.parent.pointers) - 1:
                    other_node = __node.parent.pointers[-2]
                    previous = True
                    K = __node.parent.keys[index - 1]
                else:
                    K = __node.parent.keys[index]
                    other_node = __node.parent.pointers[index + 1]
                    __index = index + 1

        if (other_node.is_leaf == True and len(other_node.keys) + len(__node.keys) < N) or \
                (other_node.is_leaf == False and len(other_node.pointers) +
                 len(__node.pointers) <= N):
            if previous == True:
                if other_node.is_leaf == False:
                    other_node.pointers = other_node.pointers + __node.pointers
                    other_node.keys = other_node.keys + [K] + __node.keys
                    for __node__ in __node.pointers:
                        __node__.parent = other_node
                else:
                    other_node.pointers = other_node.pointers[0:-1]
                    other_node.pointers = other_node.pointers + __node.pointers
                    other_node.keys = other_node.keys + __node.keys
                __node.parent.pointers = __node.parent.pointers[0:-1]
                __node.parent.keys = __node.parent.keys[0:-1]
                maintain_B_plus_tree_after_delete(table, __node.parent)
            else:
                if other_node.is_leaf == False:
                    __node.pointers = __node.pointers + other_node.pointers
                    __node.keys = __node.keys + [K] + other_node.keys
                    for __node__ in other_node.pointers:
                        __node__.parent = __node
                else:
                    __node.pointers = __node.pointers[0:-1]
                    __node.pointers = __node.pointers + other_node.pointers
                    __node.keys = __node.keys + other_node.keys
                __node.parent.pointers.pop(__index)
                __node.parent.keys.pop(__index - 1)
                maintain_B_plus_tree_after_delete(table, __node.parent)
        else:
            if previous == True:
                if other_node.is_leaf == True:
                    __node.keys.insert(0, other_node.keys.pop(-1))
                    __node.pointers.insert(0, other_node.pointers.pop(-2))
                    __node.parent.keys[-1] = __node.keys[0]
                else:
                    __tmp = other_node.pointers.pop(-1)
                    __tmp.parent = __node
                    __node.pointers.insert(0, __tmp)
                    __node.keys.insert(0, __node.parent.keys[-1])
                    __node.parent.keys[-1] = other_node.keys.pop(-1)
            else:
                if other_node.is_leaf == True:
                    __node.keys.insert(-1, other_node.keys.pop(0))
                    __node.pointers.insert(-2, other_node.pointers.pop(0))
                    __node.parent.keys[__index - 1] = other_node.keys[0]
                else:
                    __tmp = other_node.pointers.pop(0)
                    __tmp.parent = __node
                    __node.pointers.insert(-1, __tmp)
                    __node.keys.insert(-1, __node.parent.keys[__index - 1])
                    __node.parent.keys[__index - 1] = other_node.keys.pop(0)


# done
def create_index(index_name, table, column):
    pass


# done
def print_select(columns_list,columns_list_num):
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


def select_from_table(table_name, attributes, where):
    results = []
    columns = {}
    for i, col in enumerate(Catalog.tables[table_name].columns):
        columns[col.column_name] = i
    __primary_key = Catalog.tables[table_name].primary_key
    # __primary_key = 0
    # columns = {'num': 0, 'val': 1}

    if len(tables[table_name].keys) == 0:
        pass
    else:
        if where is not None:
            nodes = find_leaf_place_with_condition(table_name, columns[where[0]['l_op']], where[0]['operator'], where[0]['r_op'])
            for cond in where:
                if columns[cond['l_op']] == __primary_key:
                    nodes = find_leaf_place_with_condition(table_name, columns[cond['l_op']], cond['operator'], cond['r_op'])
                    break
            for __node in nodes:
                for pointer in __node.pointers[0:-1]:
                    if check_conditions(pointer, columns, where):
                        results.append(pointer)
        else:
            first_leaf_node = tables[table_name]
            while first_leaf_node.is_leaf != True:
                first_leaf_node = first_leaf_node.pointers[0]
            while True:
                for i in first_leaf_node.pointers[0:-1]:
                    results.append(i)
                if first_leaf_node.pointers[-1] != '':
                    first_leaf_node = first_leaf_node.pointers[-1]
                else:
                    break

    if attributes[0] == '*':
        __columns_list = list(columns.keys())
        __columns_list_num = list(columns.values())
    else:
        __columns_list = []
        __columns_list_num = []
        for i in range(0,len(attributes)):
            __columns_list.append(attributes[i])
            __columns_list_num.append(columns(attributes[i]))

    print_select(__columns_list,__columns_list_num)


# done
def check_unique(table_name, column, value):
    columns = []
    for col in Catalog.tables[table_name].columns:
        columns.append(col)
    if len(find_leaf_place_with_condition(table_name, column, '=', value)):
        raise Exception("Index Module : column '%s' does not satisfy "
                        "unique constrains." % columns[column])


# done
def find_leaf_place(table, value):
    # search on primary key
    cur_node = tables[table]
    while not cur_node.is_leaf:
        seed = False
        for index, key in enumerate(cur_node.keys):
            if key > value:
                cur_node = cur_node.pointers[index]
                seed = True
                break
        if seed == False:
            cur_node = cur_node.pointers[-1]
    return cur_node


# done
def find_leaf_place_with_condition(table_name, column, condition, value):
    # __primary_key = CatalogManager.catalog.tables[table].primary_key
    __primary_key = 0
    head_node = tables[table_name]
    first_leaf_node = head_node
    while first_leaf_node.is_leaf != True:
        first_leaf_node = first_leaf_node.pointers[0]
    lists = []
    op_list = ['<','<=','>','>=','<>','=']
    if __primary_key == column and condition != op_list[4]:
        while not head_node.is_leaf:
            seed = False
            for index, key in enumerate(head_node.keys):
                if key > value:
                    head_node = head_node.pointers[index]
                    seed = True
                    break
            if seed == False:
                head_node = head_node.pointers[-1]
        if condition == op_list[5]:
            for pointer in head_node.pointers[0:-1]:
                if pointer[column] == value:
                    lists.append(head_node)
        elif condition == op_list[1]:
            cur_node = first_leaf_node
            while True:
                if cur_node != head_node:
                    lists.append(cur_node)
                    cur_node = cur_node.pointers[-1]
                else:
                    break
            for pointer in head_node.pointers[0:-1]:
                if pointer[column] <= value:
                    lists.append(head_node)
                    break
        elif condition == op_list[0]:
            cur_node = first_leaf_node
            while True:
                if cur_node != head_node:
                    lists.append(cur_node)
                    cur_node = cur_node.pointers[-1]
                else:
                    break
            for pointer in head_node.pointers[0:-1]:
                if pointer[column] < value:
                    lists.append(head_node)
                    break
        elif condition == op_list[2]:
            for pointer in head_node.pointers[0:-1]:
                if pointer[column] > value:
                    lists.append(head_node)
                    break
            while True:
                head_node = head_node.pointers[-1]
                if head_node != '':
                    lists.append(head_node)
                else:
                    break
        elif condition == op_list[3]:
            for pointer in head_node.pointers[0:-1]:
                if pointer[column] >= value:
                    lists.append(head_node)
                    break
            while True:
                head_node = head_node.pointers[-1]
                if head_node != '':
                    lists.append(head_node)
                else:
                    break
        else:
            raise Exception("Index Module : unsupported op.")

    else:
        if first_leaf_node.pointers:
            while True:
                for pointer in first_leaf_node.pointers[0:-1]:
                    if condition not in op_list:
                        raise Exception("Index Module : unsupported op.")
                    else:
                        if condition == op_list[0]:
                            if pointer[column] < value:
                                lists.append(first_leaf_node)
                                break
                        elif condition == op_list[1]:
                            if pointer[column] <= value:
                                lists.append(first_leaf_node)
                                break
                        elif condition == op_list[2]:
                            if pointer[column] > value:
                                lists.append(first_leaf_node)
                                break
                        elif condition == op_list[3]:
                            if pointer[column] >= value:
                                lists.append(first_leaf_node)
                                break
                        elif condition == op_list[4]:
                            if pointer[column] != value:
                                lists.append(first_leaf_node)
                                break
                        elif condition == op_list[5]:
                            if pointer[column] == value:
                                lists.append(first_leaf_node)
                                break
                if first_leaf_node.pointers[-1] == '':
                    break
                first_leaf_node = first_leaf_node.pointers[-1]
    return lists


# done
def insert_into_leaf(cur_node, value, pointer):
    for index, key in enumerate(cur_node.keys):
        if key == value:
            raise Exception("Index Module : primary_key already exists.")
        if key > value:
            cur_node.pointers.insert(index, pointer)
            cur_node.keys.insert(index, value)
            return
    cur_node.pointers.insert(len(cur_node.keys), pointer)
    cur_node.keys.insert(len(cur_node.keys), value)


# done
def insert_into_parent(table_name, __node, __key, new_node):
    if __node.parent == '':
        cur_node = node(False, [], [],[], '')
        cur_node.pointers.append(__node)
        cur_node.pointers.append(new_node)
        cur_node.keys.append(__key)
        __node.parent = cur_node
        new_node.parent = cur_node
        tables[table_name] = cur_node
    else:
        p = __node.parent
        if len(p.pointers) < N:
            seed = False
            for index, key in enumerate(p.keys):
                if __key < key:
                    p.keys.insert(index, __key)
                    p.pointers.insert(index + 1, new_node)
                    seed = True
                    break
            if seed == False:
                p.keys.append(__key)
                p.pointers.append(new_node)
            new_node.parent = p
        else:
            seed = False
            for index, key in enumerate(p.keys):
                if __key < key:
                    p.keys.insert(index, __key)
                    p.pointers.insert(index + 1, new_node)
                    seed = True
                    break
            if seed == False:
                p.keys.append(__key)
                p.pointers.append(new_node)
            __new_node = node(False, [], [],[])
            tmp_keys = p.keys
            tmp_pointers = p.pointers
            p.keys = []
            p.pointers = []
            for i in range(math.ceil(N / 2)):
                p.keys.append(tmp_keys.pop(0))
                p.pointers.append(tmp_pointers.pop(0))
            p.pointers.append(tmp_pointers.pop(0))
            k__ = tmp_keys.pop(0)
            for i in range(N - math.ceil(N / 2) - 1):
                __new_node.keys.append(tmp_keys.pop(0))
                __tmp = tmp_pointers.pop(0)
                __tmp.parent = __new_node
                __new_node.pointers.append(__tmp)
            __tmp = tmp_pointers.pop(0)
            __tmp.parent = __new_node
            __new_node.pointers.append(__tmp)
            new_node.parent = __new_node
            insert_into_parent(table_name, p, k__, __new_node)
