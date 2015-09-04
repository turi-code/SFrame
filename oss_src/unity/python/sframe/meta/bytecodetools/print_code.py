'''
Created on May 10, 2012

@author: sean
'''
from .bytecode_consumer import ByteCodeConsumer
from argparse import ArgumentParser

class ByteCodePrinter(ByteCodeConsumer):
    
    def generic_consume(self, instr):
        print instr

def main():
    parser = ArgumentParser()
    parser.add_argument()

if __name__ == '__main__':
    main()
