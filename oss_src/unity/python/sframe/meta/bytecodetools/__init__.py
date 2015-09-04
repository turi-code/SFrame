'''
Python byte-code tools expands on the Python dis module.

'''


from .instruction import Instruction

from .disassembler_ import disassembler

from .bytecode_consumer import ByteCodeConsumer, StackedByteCodeConsumer

