BUFFER_SIZE = 10  # lines

class Buffer(object):
    def __init__(self, id: int, table: str, line: int):
        self.id = id
        self.table = table
        self.line = line

    __content = []

buffers = []