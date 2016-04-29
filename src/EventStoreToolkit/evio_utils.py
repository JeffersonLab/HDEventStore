
import struct

def EVIO_SWAP64(x):
    return ( (((x) >> 56) & 0x00000000000000FFL) | \
                 (((x) >> 40) & 0x000000000000FF00L) | \
                 (((x) >> 24) & 0x0000000000FF0000L) | \
                 (((x) >> 8)  & 0x00000000FF000000L) | \
                 (((x) << 8)  & 0x000000FF00000000L) | \
                 (((x) << 24) & 0x0000FF0000000000L) | \
                 (((x) << 40) & 0x00FF000000000000L) | \
                 (((x) << 56) & 0xFF00000000000000L) )


def EVIO_SWAP32(x):
    return ( (((x) >> 24) & 0x000000FF) | \
                 (((x) >> 8)  & 0x0000FF00) | \
                 (((x) << 8)  & 0x00FF0000) | \
                 (((x) << 24) & 0xFF000000) )


def GetRunNumberFromFilename(fileName):
    try:
        # assume raw data has file name of form "hd_{raw,rawdata}_RRRRRR_FFF.evio"
        path_elements = fileName.split('/')
        filename_elements = path_elements[-1].split('_')
        return int(filename_elements[2])
    except:
        return -1

"""
class buffered_reader:
    def __init__(self,filename,chunksize=8192):
        self.filename = filename
        self.chunksize = chunksize

    def words_from_file(filename, chunksize=8192):
        with open(filename, "rb") as f:
            while True:
                chunk = f.read(chunksize)
            if chunk:
                for b in chunk:
                    yield b
            else:
                break
"""
