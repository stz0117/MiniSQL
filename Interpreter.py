import os
import re
from cmd import Cmd

import API


def auto_type(value: str):
    if value[0] == "'" and value[-1] == "'":
        value = value[1:-1]
    elif re.match(r'^-?[0-9]+\.+[0-9]+$', value):
        value = float(value)
    elif re.match(r'^-?[0-9]+', value):
        value = int(value)
    else:
        raise Exception(f"Unsupported format: {value}")
    return value


# 分离出单独的方法，方便execfile调用
def create(arg: str):
    arg = arg.strip()

    if arg[-1] == '(':  # 支持create的多行输入
        while 1:
            line = input().rstrip()
            arg = arg + line
            if line[-1] != ',' and line[-1] != ')':
                break

    arg = arg.rstrip(';').strip()  # 去尾部分号
    arg = re.sub(' +', ' ', arg)  # 将多空格换为单空格（两边是没有空格的）

    if arg[:5] == 'table':
        arg = arg[5:]
        arg = arg.lstrip()  # 去table后空格

        table_name = arg[:arg.find('(')].strip()  # 通过定位'('获取表名
        if table_name == '':
            raise Exception("No table name found.")

        # 去除定义表的括号
        arg = arg[arg.find('('):]
        arg = arg.lstrip('(').strip()
        if arg[-1] == ')':
            arg = arg[:-1]
        arg = arg.strip()
        if arg == '':
            raise Exception("No table specification found.")

        # 获取属性定义、pk等
        attribute_specifications = arg.split(',')
        attribute_specifications = list(map(str.strip, attribute_specifications))
        if attribute_specifications == []:
            raise Exception("No table attribute found.")

        # 先处理pk
        pk = None
        if attribute_specifications[-1].startswith('primary key'):
            pk = attribute_specifications[-1]
            if ',' in pk:
                raise Exception("Only single primary key is supported.")
            pk = pk[11:].strip().lstrip('(').rstrip(')').strip()
            attribute_specifications = attribute_specifications[:-1]

        # 依序处理属性定义
        attributes = []
        attribute_names = []
        for attribute_specification in attribute_specifications:
            # item: attribute name, type, and optional unique
            unique = False
            type_len = 0
            item = attribute_specification.split(' ')

            if item[1] not in ['int', 'float']:
                if item[1].startswith('char'):
                    type_len = int(item[1][4:].strip().lstrip('(').rstrip(')'))
                    if type_len <= 0:
                        raise Exception(f"The size of the type is negative.")
                    item[1] = 'char'
                else:
                    raise Exception(f"The type of attribute {item[0]} is {item[1]}, which is not supported.")

            if len(item) == 3:
                if item[2] == "unique":
                    unique = True
                else:
                    raise Exception(f"The command behind {item[0]} {item[1]} is not supported.")

            attribute_names.append(item[0])
            attributes.append({
                'attribute_name': item[0],
                'type': item[1],
                'type_len': type_len,
                'unique': unique
            })

        if pk:
            if pk not in attribute_names:
                raise Exception(f"The primary key {pk} you want is not in the attribute list.")
            else:
                attributes[attribute_names.index(pk)]['unique'] = True
        print(table_name, attributes, pk)
        API.create_table(table_name, attributes, pk)

    elif arg[:5] == 'index':
        arg = arg[5:]
        arg = arg.lstrip()  # 去index后空格

        location_on = arg.find('on')
        if location_on == -1:
            raise Exception(f"'on' is missing when creating index.")
        index_name = arg[:location_on].strip()

        location_lbracket = arg.find('(')
        if location_lbracket == -1:
            raise Exception(f"Indexed attribute format is wrong.")
        table_name = arg[location_on + len('on'): location_lbracket].strip()

        location_rbracket = arg.find(')')
        if location_rbracket == -1:
            raise Exception(f"Indexed attribute format is wrong.")
        indexed_attr = arg[location_lbracket + 1: location_rbracket].strip()

        if ',' in indexed_attr:
            raise Exception('Only single attribute index is supported.')
        print(index_name, table_name, indexed_attr)
        API.create_index(index_name, table_name, indexed_attr)
    else:
        raise Exception("The item you want to create is not supported.")


def drop(arg: str):
    arg = arg.rstrip(';')
    arg = re.sub(' +', ' ', arg).strip()
    if arg[:5] == 'table':
        arg = arg[5:].strip()
        print(arg)
        API.drop_table(arg)
    elif arg[:5] == 'index':
        arg = arg[5:].strip()
        print(arg)
        API.drop_index(arg)
    else:
        raise Exception("The item you want to drop is not supported.")


def select(arg: str):
    arg = arg.rstrip(';')
    arg = re.sub(' +', ' ', arg).strip()

    location_from = arg.find('from')
    if location_from == -1:
        raise Exception("Table name is missing.")
    attributes = arg[:location_from].strip().split(',')
    attributes = list(map(str.strip, attributes))

    location_where = arg.find('where')
    if location_where == -1:
        table_name = arg[location_from + len('from'):].strip()
        print(table_name, attributes)
        API.select(table_name, attributes)
    else:
        table_name = arg[location_from + len('from'):location_where].strip()
        conditions = arg[location_where + len('where'):].strip().split('and')
        conditions = list(map(str.strip, conditions))
        where = []
        for condition in conditions:
            operators = ['<>', '<=', '>=', '=', '<', '>']
            no_operator = True
            for operator in operators:
                if operator in condition:
                    op = operator
                    location = condition.find(op)
                    l_op = condition[:location].strip()
                    r_op = condition[location + len(op):].strip()
                    no_operator = False
                    break
            if no_operator:
                raise Exception(f"no operator found in {condition}")
            r_op = auto_type(r_op)
            where.append({'operator': operator, 'l_op': l_op, 'r_op': r_op})
        print(table_name, attributes, where)
        API.select(table_name, attributes, where)


def insert(arg: str):
    arg = arg.rstrip(';')
    arg = re.sub(' +', ' ', arg).strip()

    location_into = arg.find('into')
    if location_into == -1:
        raise Exception("'into' is missing.")

    location_values = arg.find('values')
    if location_values == -1:
        raise Exception("'values' is missing.")

    table_name = arg[location_into + 4: location_values].strip()

    location_lbracket = arg.find('(')
    location_rbracket = arg.find(')')
    values = arg[location_lbracket + 1: location_rbracket].split(',')
    values = list(map(str.strip, values))
    values = list(map(auto_type, values))
    print(table_name, values)
    API.insert(table_name, values)


def delete(arg: str):
    arg = arg.rstrip(';')
    arg = re.sub(' +', ' ', arg).strip()

    location_from = arg.find('from')
    if location_from == -1:
        raise Exception("'from' is missing.")

    location_where = arg.find('where')
    if location_where == -1:
        table_name = arg[location_from + 4:].strip()
        print(table_name)
        API.delete(table_name)
    else:
        table_name = arg[location_from + 4: location_where].strip()
        conditions = arg[location_where + len('where'):].strip().split('and')
        conditions = list(map(str.strip, conditions))
        where = []
        for condition in conditions:
            operators = ['=', '<>', '<', '>', '<=', '>=']
            no_operator = True
            for operator in operators:
                if operator in condition:
                    op = operator
                    location = condition.find(op)
                    l_op = condition[:location].strip()
                    r_op = condition[location + 1:].strip()
                    no_operator = False
                    break
            if no_operator:
                raise Exception(f"no operator found in {condition}")
            r_op = auto_type(r_op)
            where.append({'operator': operator, 'l_op': l_op, 'r_op': r_op})
        print(table_name, where)
        API.delete(table_name, where)


def show(arg: str):
    arg = arg.rstrip(';')
    arg = re.sub(' +', ' ', arg).strip()

    location_tables = arg.find('tables')
    if location_tables == -1:
        location_table = arg.find('table')
        if location_table == -1:
            raise Exception(f"The item you want to show is not supported.")
        else:
            table_name = arg[location_table + 5:].strip()
            print(table_name)
            API.show_table(table_name)
    else:
        API.show_tables()


class Interpreter(Cmd):
    prompt = "MiniSQL> "
    intro = "Welcome to our MiniSQL project!"

    def __init(self):
        Cmd.__init__(self)

    def preloop(self):
        API.initialize(os.getcwd())

    def do_create(self, arg: str):
        try:
            create(arg)
        except Exception as e:
            print(e)

    def do_drop(self, arg: str):
        try:
            drop(arg)
        except Exception as e:
            print(e)

    def do_select(self, arg: str):
        try:
            select(arg)
        except Exception as e:
            print(e)

    def do_insert(self, arg: str):
        try:
            insert(arg)
        except Exception as e:
            print(e)

    def do_delete(self, arg: str):
        try:
            delete(arg)
        except Exception as e:
            print(e)

    def do_show(self, arg: str):
        try:
            show(arg)
        except Exception as e:
            print(e)

    def do_commit(self, arg: str):
        API.save()

    def do_execfile(self, arg: str):
        switch = {
            'create': create,
            'drop': drop,
            'select': select,
            'insert': insert,
            'delete': delete,
            'show': show
        }
        i = 1
        try:
            f = open(arg.strip(';').strip(), 'r')
            while 1:
                line = f.readline().strip()
                if line == '':
                    break
                if line[0] == '#':
                    i += 1
                    continue
                command = line[:line.find(' ')]
                arg = line[line.find(' '):]
                switch[command](arg)
                i += 1
        except Exception as e:
            print(f"An exception occurred at line {i}:")
            print(e)

    def do_exit(self, arg: str):
        API.save()
        print('Bye~')
        return True

    def emptyline(self):
        pass

    def default(self, line: str):
        print(f"Unknown command: {line.split(' ')[0]}")


if __name__ == "__main__":
    Interpreter().cmdloop()
