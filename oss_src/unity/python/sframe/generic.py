'''
Copyright (C) 2016 Turi
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
class CommonEqualityMixin(object):
    """
    Enable subclasses of this class to do equality comparison.

    Code taken from http://stackoverflow.com/questions/390250/elegant-ways-to-support-equivalence-equality-in-python-classes
    """
    def __eq__(self, other):
        return (isinstance(other, self.__class__)
                and self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)
