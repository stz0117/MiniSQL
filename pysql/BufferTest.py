# DO NOT DIRECTLY RUN THIS!!
# copy some of those lines to the python console to do some test
import struct

f = open('pysql/dbfiles/table_files/table_S.bin', 'rb+')

# S (ID int , name char(12) unique, age int, gender char(1), primary key (ID));
# a line of S:
# < means small endian
lyxt = struct.pack('<ci12si1s', b'\x00', 201801001, b'lyxt', 20, b'F')
stz = struct.pack('<ci12si1s', b'\x00', 201801002, b'stz', 20, b'M')
hsw = struct.pack('<ci12si1s', b'\x00', 201801003, b'hsw', 20, b'M')
yjx = struct.pack('<ci12si1s', b'\x00', 201801004, b'yjx', 20, b'M')
zyx = struct.pack('<ci12si1s', b'\x00', 201801005, b'zyx', 20, b'M')
zdy = struct.pack('<ci12si1s', b'\x00', 201801006, b'zdy', 20, b'M')
zdw = struct.pack('<ci12si1s', b'\x00', 201801007, b'zdw', 20, b'M')
slj = struct.pack('<ci12si1s', b'\x00', 201801008, b'slj', 20, b'F')
myn = struct.pack('<ci12si1s', b'\x00', 201801009, b'myn', 20, b'F')
lxz = struct.pack('<ci12si1s', b'\x00', 201801010, b'lxz', 19, b'F')
cyj = struct.pack('<ci12si1s', b'\x00', 201801011, b'cyj', 20, b'F')

l = [lyxt, stz, hsw, yjx, zyx, zdy, zdw, slj, myn, lxz, cyj]
for i in l:
    f.write(i)
