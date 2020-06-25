import struct
import os
import Catalog
import Index
# from Catalog import tables

# For the convenience of test and presentation, the buffer size is set to a small, static number.
# In real application, we can make it as a var of Buffer object, adjust it according to the size of line,
# making the size of buf (BUFFER_SIZE * line_size) nearly 4KB/8KB for the sake of block transfer.
BUFFER_SIZE = 8  # number of lines
BUFFER_NUM = 2  # for each table


# ---------------------- for easy internal test --------------------------
'''
class Table(object):  # data structure to save a table
    def __init__(self, table_name, pk=0):
        self.table_name = table_name
        self.primary_key = pk

    columns = []


class Column(object):  # data structure to save an attribute
    def __init__(self, column_name, is_unique, type='char', length=16):
        self.column_name = column_name
        self.is_unique = is_unique
        self.type = type
        self.length = length


# S (ID int , name char(12) unique, age int, gender char(1), primary key (ID));
tables = {'S': Table('S', 0)}
tables['S'].columns = [
    Column('ID', True, 'int', 0),
    Column('name', True, 'char', 12),
    Column('age', False, 'int', 0),
    Column('gender', False, 'char', 1)
]
'''
# ---------------------------------------------------------------------

buffers = {}  # key: name, value: buffer of this table


class Buffer(object):
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.file_line = 0  # the position of buffer's first line in actual file
        self.is_dirty = False
        self.pin = False
        self.format_list = ['<c']
        self.line_size = 1  # we have a flag of 1 byte indicating record (0x00) or pointer (0x01)

        # get the format char to support pack and unpack
        for column in Catalog.tables[table_name].columns:
            if column.type == 'int':
                self.format_list += ['i']
                self.line_size += 4
            elif column.type == 'float':
                self.format_list += ['f']
                self.line_size += 4
            elif column.type == 'char':
                self.format_list += [f'{column.length}s']
                self.line_size += column.length
        if self.line_size < 5:
            self.line_size = 5  # we need 4 extra bytes at least to store the empty line pointer

        f = open(f'dbfiles/table_files/table_{self.table_name}.bin', 'rb')
        self.buf_size = BUFFER_SIZE  # as I mentioned before, it can be adjusted according to the real application
        # self.buf_size = 4096 // self_size  # such as this
        self.cur_size = BUFFER_SIZE
        self.content = []

        # fill the buffer
        for i in range(self.buf_size):
            line = f.read(self.line_size)
            if line == b'':
                self.cur_size = i
                break
            self.content.append(line)
        f.close()
        self.ins_pos = struct.unpack('<I', self.content[0][1:5])[0]

    def adjust(self, line_number):
        if self.pin:
            raise Exception("Locked buffer is not allowed to replace!")
        self.save()
        f = open(f'dbfiles/table_files/table_{self.table_name}.bin', 'rb')
        # set a different start point
        f.seek(line_number * self.line_size)
        self.cur_size = BUFFER_SIZE
        self.content = []
        for i in range(self.buf_size):
            line = f.read(self.line_size)
            if line == b'':
                self.cur_size = i
                break
            self.content.append(line)
        self.file_line = line_number
        f.close()

    def save(self):
        f = open(f'dbfiles/table_files/table_{self.table_name}.bin', 'wb+')
        f.seek(self.file_line * self.line_size)
        for line in self.content:
            f.write(line)
        # save the insert position as well
        f.seek(0)
        f.write(struct.pack(f'<cI{self.line_size - 5}s', b'\x01', self.ins_pos, b'\x00' * (self.line_size - 5)))
        f.close()
        self.is_dirty = False

    def is_full(self):
        return self.cur_size == self.buf_size

    def line_is_inside(self, n: int):
        return self.file_line <= n < self.file_line + self.cur_size


def __initialize__():
    global buffers

    if not os.path.exists(f'dbfiles/table_files'):
        os.makedirs(f'dbfiles/table_files')

    for table in Catalog.tables.values():
        buffers[table.table_name] = Buffer(table.table_name)



def __finalize__():
    global buffers
    for buffer in buffers.values():
        buffer.save()


def check(line: list, columns: dict, where: list):
    for cond in where:
        column = columns[cond['l_op']]
        value_l = line[column]
        if cond['operator'] == '<>':
            if not value_l != cond['r_op']:
                return False
        elif cond['operator'] == '<=':
            if not value_l <= cond['r_op']:
                return False
        elif cond['operator'] == '>=':
            if not value_l >= cond['r_op']:
                return False
        elif cond['operator'] == '=':
            if not value_l == cond['r_op']:
                return False
        elif cond['operator'] == '<':
            if not value_l < cond['r_op']:
                return False
        elif cond['operator'] == '>':
            if not value_l > cond['r_op']:
                return False
        else:
            raise Exception("No such operation.")
    return True


def decode(format_str: str, line: bytes):
    line = list(struct.unpack(format_str, line))
    for i, item in enumerate(line):
        if isinstance(item, bytes):
            line[i] = line[i].decode('utf-8').rstrip('\x00')
    return line[1:]


def find_attr_pos(table_name: str, attribute: str):
    global tables
    pos = -1
    for i, col in enumerate(Catalog.tables[table_name].columns):
        if col.column_name == attribute:
            pos = i
    return pos


# 'S' 5
# because we store the line number in the index
# we can find a particular line directly
def find_line(table_name: str, line_number: int):
    global buffers
    buffer = buffers[table_name]
    # if the line is not in buffer, we need to fetch the page first
    if not buffer.line_is_inside(line_number):
        buffer.adjust(line_number)
        if not buffer.line_is_inside(line_number):
            raise Exception("The line you want to retrieve exceeds the file.")

    line = buffer.content[line_number - buffer.file_line]
    if line[0] == 1:
        raise Exception("The line you want to retrieve is not existed.")
    line = decode(''.join(buffer.format_list), line)
    return line


# 'S' 'gender' '==' 'F'
# def find_record(table_name: str, attribute: str, cond: str, value):
def find_record(table_name: str, columns: dict, where: list):
    # first find them in buffer
    global buffers
    buffer = buffers[table_name]
    results = []
    buffer_range = range(buffer.file_line, buffer.file_line + buffer.cur_size)
    for line in buffer.content:
        line = decode(''.join(buffer.format_list), line)
        if check(line, columns, where):
            results += [line]
    # then find them in file (no fetch)
    f = open(f'dbfiles/table_files/table_{table_name}.bin', 'rb')
    f.seek(buffer.line_size)
    i = 0
    while 1:
        i += 1
        # skip the line already scanned in the buffer
        if i in buffer_range:
            i = buffer_range[-1]
            f.seek(buffer.line_size * (i + 1))
            continue
        line = f.read(buffer.line_size)
        # reach EOF
        if line == b'':
            f.close()
            break
        # a pointer line
        if line[0] == 1:
            continue
        else:
            line = decode(''.join(buffer.format_list), line)
            if check(line, columns, where):
                results += [line]
    return results


def delete_line(table_name: str, line_number: int):
    global buffers
    buffer = buffers[table_name]
    remain = struct.pack(f'<cI{buffer.line_size - 5}s', b'\x01', buffer.ins_pos, b'\x00' * (buffer.line_size - 5))
    # if the line is not in buffer, we need to fetch the page first
    if not buffer.line_is_inside(line_number):
        buffer.adjust(line_number)
        if not buffer.line_is_inside(line_number):
            raise Exception("The line you want to retrieve exceeds the file.")

    if buffer.content[line_number - buffer.file_line][0] == 1:
        raise Exception("The line you want to retrieve is not existed.")
    buffer.content[line_number - buffer.file_line] = remain
    buffer.is_dirty = True
    buffer.ins_pos = line_number


def delete_record(table_name: str, column: dict, where: list):
    pk_pos = Catalog.tables[table_name].primary_key
    # first search in the buffer
    global buffers
    buffer = buffers[table_name]
    buffer_range = range(buffer.file_line, buffer.file_line + buffer.cur_size)
    deleted_pks = []
    for i, line in enumerate(buffer.content):
        line = decode(''.join(buffer.format_list), line)
        if check(line, column, where):
            deleted_pks += [line[pk_pos]]
            buffer.content[i] = \
                struct.pack(f'<cI{buffer.line_size - 5}s',
                            b'\x01', buffer.ins_pos,
                            b'\x00' * (buffer.line_size - 5))
            buffer.ins_pos = buffer.file_line + i
            buffer.is_dirty = True
    # then search in the file (no fetch)
    f = open(f'dbfiles/table_files/table_{table_name}.bin', 'rb+')
    f.seek(buffer.line_size)
    i = 0
    while 1:
        i += 1
        if i in buffer_range:
            i = buffer_range[-1]
            f.seek(buffer.line_size * (i + 1))
            continue
        line = f.read(buffer.line_size)
        if line == b'':
            f.close()
            break
        if line[0] == 1:
            continue
        else:
            line = decode(''.join(buffer.format_list), line)
            if check(line, column, where):
                deleted_pks += [line[pk_pos]]
                f.seek(buffer.line_size * i)
                f.write(
                    struct.pack(f'<cI{buffer.line_size - 5}s',
                                b'\x01',
                                buffer.ins_pos,
                                b'\x00' * (buffer.line_size - 5)))
                buffer.ins_pos = i
    return deleted_pks
    # when you doing conditional delete, first call Buffer.delete_record,
    # it will tell you pks of the records which are deleted


def check_unique(table_name: str, line_size: int, line: bytes):
    global tables, buffers
    table = Catalog.tables[table_name]
    buffer = buffers[table_name]
    buffer_range = range(buffer.file_line, buffer.file_line + buffer.cur_size)
    unique = []
    i = 1
    for column in table.columns:
        if (column.type == 'int' or column.type == 'float') and column.is_unique:
            unique += [(i, i + 4)]
            i += 4
        elif column.type == 'char' and column.is_unique:
            unique += [(i, i + column.length)]
            i += column.length
    # first search in the buffer
    if buffer.is_dirty:
        for ln in buffer.content:
            for i, j in unique:
                if ln[i:j] == line[i:j]:
                    raise Exception("Unique constraint is not conserved.")
    # then search in the file (no fetch)
    f = open(f'dbfiles/table_files/table_{table_name}.bin', 'rb')
    f.seek(line_size)
    i = 0
    while 1:
        i += 1
        if i in buffer_range:
            i = buffer_range[-1]
            f.seek(buffer.line_size * (i + 1))
            continue
        ln = f.read(line_size)
        if ln == b'':
            f.close()
            break
        if ln[0] == 1:
            continue
        for i, j in unique:
            if ln[i:j] == line[i:j]:
                raise Exception("Unique constraint is not conserved.")


def insert_record(table_name: str, record: []):
    for i, item in enumerate(record):
        if isinstance(item, str):
            record[i] = record[i].encode('utf-8')

    line = b''
    line += struct.pack('<c', b'\x00')
    global buffers
    buffer = buffers[table_name]
    for i, item in enumerate(record):
        line += struct.pack(buffer.format_list[i + 1], item)

    check_unique(table_name, buffer.line_size, line)
    if not buffer.line_is_inside(buffer.ins_pos):
        buffer.adjust(buffer.ins_pos)
    # if we are inserting to the last line of the file
    # the buffer will be empty
    # so the operation is different
    tmp = buffer.ins_pos
    if buffer.cur_size == 0:
        buffer.ins_pos += 1
        buffer.content = [line]
        buffer.cur_size = 1
    else:
        buffer.ins_pos = struct.unpack('<cI', buffer.content[buffer.ins_pos - buffer.file_line][0:5])[1]
        buffer.content[tmp - buffer.file_line] = line
    buffer.is_dirty = True
    return tmp
    # when you insert a node, first call Buffer.insert_record,
    # it will tell you which line the record is stored


def create_table(table_name: str):
    global tables
    f = open(f'dbfiles/table_files/table_{table_name}.bin', 'wb')

    line_size = 1
    for column in Catalog.tables[table_name].columns:
        if column.type == 'int':
            line_size += 4
        elif column.type == 'float':
            line_size += 4
        elif column.type == 'char':
            line_size += column.length
    if line_size < 5:
        line_size = 5  # we need 4 extra bytes at least to store the empty line pointer
    # the new file only contains a head, which points to the second line (line 1 if we count from 0)
    f.write(struct.pack(f'<cI{line_size - 5}s', b'\x01', 1, b'\x00' * (line_size - 5)))
    f.close()
    # then create a buffer for it
    buffers[table_name] = Buffer(table_name)


def drop_table(table_name: str):
    global buffers
    buffers.pop(table_name)
    os.remove(f'dbfiles/table_files/table_{table_name}.bin')


def pin_buffer(table_name: str):
    buffers[table_name].pin = True


def unpin_buffer(table_name: str):
    buffers[table_name].pin = False

'''
__initialize__()
# a = find_line('S', 5)
# print(a)
# a = find_line('S', 9)
# print(a)

column = {'ID': 0, 'name': 1, 'age': 2, 'gender': 3}
where = [{'operator': '=', 'l_op': 'gender', 'r_op': 'F'}, {'operator': '<', 'l_op': 'ID', 'r_op': 201801009}]

# for rec in find_record('S', column, where):
#     print(rec)

# delete_line('S', 5)
a=['ID','d','d','v']
b=[0,1,2,3]
results=find_record('S', column, where)

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


print_select(a,b)

# print(delete_record('S', column, where))


# insert_record('S', [201801005, 'zyx', 20, 'M'])
# insert_record('S', [201801011, 'cyj', 20, 'F'])
# create_table('S')
# # drop_table('S')
__finalize__()
print('123')
print(results.__len__())
'''
