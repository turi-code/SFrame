'''
Created on Aug 3, 2011

@author: sean
'''

import _ast
from ...asttools.visitors import Visitor

class Replacer(Visitor):
    '''
    Visitor to replace nodes. 
    '''

    def __init__(self, old, new):
        self.old = old
        self.new = new

    def visitDefault(self, node):
        for field in node._fields:
            value = getattr(node, field)

            if value == self.old:
                setattr(node, field, self.new)

            if isinstance(value, (list, tuple)):
                for i, item in enumerate(value):
                    if item == self.old:
                        value[i] = self.new
                    elif isinstance(item, _ast.AST):
                        self.visit(item)
                    else:
                        pass
            elif isinstance(value, _ast.AST):
                self.visit(value)
            else:
                pass

        return

def replace_nodes(root, old, new):

    '''
    Replace the old node with the new one. 
    Old must be an indirect child of root
     
    :param root: ast node that contains an indirect refrence to old 
    :param old: node to replace
    :param new: node to replace `old` with 
    '''

    rep = Replacer(old, new)
    rep.visit(root)
    return

class NodeRemover(Visitor):
    '''
    Remove a node.
    '''
    def __init__(self, to_remove):
        self.to_remove

    def visitDefault(self, node):
        for field in node._fields:
            value = getattr(node, field)

            if value in self.to_remove:
                setattr(node, field, self.new)

            if isinstance(value, (list, tuple)):
                for i, item in enumerate(value):
                    if item == self.old:
                        value[i] = self.new
                    elif isinstance(item, _ast.AST):
                        self.visit(item)
                    else:
                        pass
            elif isinstance(value, _ast.AST):
                self.visit(value)
            else:
                pass

        return

