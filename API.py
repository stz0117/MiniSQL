# import CatalogManager
# import RecordManager
# import IndexManager


# an example of upcoming args
# S
# [{'attribute_name': 'ID', 'type': 'int', 'type_len': 0, 'unique': True},
# {'attribute_name': 'name', 'type': 'char', 'type_len': 12, 'unique': True},
# {'attribute_name': 'age', 'type': 'int', 'type_len': 0, 'unique': False},
# {'attribute_name': 'gender', 'type': 'char', 'type_len': 1, 'unique': False}]
# ID
def create_table(table_name: str, attributes: list, pk: str):
    pass
    # 从 CM(Catalog Manager 后同)确定表是否已经存在
    # 已存在可以抛出异常，Interpreter会接，其他无法执行的情况均抛异常
    # CatalogManager.exist_table(table_name)

    # Interpreter验证过pk是存在于attr中的

    # CatalogManager.create_table(table_name, attributes)
    # IndexManager.create_index(table_name, pk)


# e.g. name_index S name
def create_index(index_name: str, table_name: str, indexed_attr: str):
    pass
    # CatalogManager.exist_table(table_name)
    # 查重名index
    # IndexManager.exist_index(indexed_attr)
    # 查属性是否unique
    # IndexManager.is_unique(table_name, indexed_attr)
    # IndexManager.create_index(table_name, indexed_attr)


def drop_table(table_name: str):
    pass
    # CatalogManager.exist_table(table_name)
    # IndexManager.drop_index_of(table_name)
    # CatalogManager.drop_table(table_name) # 也可以在 drop_table 里面检查表是否存在


def drop_index(index_name: str):
    pass
    # IndexManager.exist_index(index_name)
    # IndexManager.drop_index(index_name)


# e.g.
# student
# ['*']
# [{'operator': '>', 'l_op': 'sage', 'r_op': 20},
# {'operator': '=', 'l_op': 'sgender', 'r_op': 'F'},
# {'operator': '<', 'l_op': 'sscore', 'r_op': 89.5}]
def select(table_name: str, attributes: list, where: dict = None):
    pass
    # CatalogManager.exist_table(table_name)
    # 注意传来的attr的顺序与表实际定义的顺序不一定相同
    # CatalogManager.is_name_valid(table_name, attributes)
    # CatalogManager.check_where(table_name, where) # 检查 where的属性是否存在、作为比较的类型是否正确（比如int,float可以互相比较，str不可）

    # 还要考虑可以利用索引的情况（单值查找且有索引的情况）
    # pseudo: if len(where) == 1 and where[0]['l_op'] is indexed
    #           positions = IndexManager.getPosition(where)
    #            RecordManager.getByPos(positions)
    # RecordManager.get(table_name, attributes, dict)


# e.g. student ['12345678', 'wy', 22, 'M']
def insert(table_name: str, values: list):
    pass
    # CatalogManager.exist_table(table_name)
    # CatalogManager.is_type_correspond(table_name, values) # 列表中的数据是否能插入（类型正确，字符串还要确保长度）
    # RecordManager.insert(table_name, values)
    # 考虑对索引的影响


# e.g. student [{'operator': '=', 'l_op': 'sno', 'r_op': '88888888'}]
def delete(table_name: str, where: list = None):
    pass
    # CatalogManager.exist_table(table_name)
    # CatalogManager.check_where(table_name, where)
    # RecordManager.delete(table_name, where)


def show_table(table_name: str):
    pass
    # CatalogManager.exist_table(table_name)
    # CatalogManager.show_table(table_name) # 显示属性名、类型、大小（char）、是否unique、是否主键、index情况等等


def show_tables():
    pass
    # CatalogManager.show_tables() # 列出所有表名
