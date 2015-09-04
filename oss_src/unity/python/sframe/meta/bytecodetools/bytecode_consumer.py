'''
Created on Apr 28, 2012

@author: sean
'''
from .disassembler_ import disassembler


class ByteCodeConsumer(object):
    '''
    ByteCodeVisitor
    '''
    def __init__(self, code):
        self.code = code
        self.byte_code = code.co_code
        
    def consume(self):
        '''
        Consume byte-code
        '''
        generic_consume = getattr(self, 'generic_consume', None)
        
        for instr in disassembler(self.code):
            method_name = 'consume_%s' % (instr.opname)
            method = getattr(self, method_name, generic_consume)
            if not method:
                raise AttributeError("class %r has no method %r" % (type(self).__name__, method_name))
            
            self.instruction_pre(instr)
            method(instr)
            self.instruction_post(instr)
            
    def instruction_pre(self, instr):
        '''
        consumer calls this instruction before every instruction.
        '''
    
    def instruction_post(self, instr):
        '''
        consumer calls this instruction after every instruction.
        '''
    

class StackedByteCodeConsumer(ByteCodeConsumer):
    '''
    A consumer with the concept of a stack.
    '''
    
    def __init__(self, code):
        ByteCodeConsumer.__init__(self, code)
        self._stack = []
        
    def pop_top(self):
        return self._stack.pop()
    
    def push(self, value):
        self._stack.append(value)
    
