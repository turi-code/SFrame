"""
GraphLab Create offers several data structures for data analysis.

Concise descriptions of the data structures and their methods are contained in
the API documentation, along with a small number of simple examples.

For more detailed descriptions and examples, please see the:
- `User Guide <https://turi.com/learn/userguide/>`_,
- `API Translator <https://turi.com/learn/translator/>`_,
- `How-Tos <https://turi.com/learn/how-to/>`_
- `Gallery <https://turi.com/learn/gallery/>`_.
"""

'''
Copyright (C) 2016 Turi
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''

__all__ = ['sframe', 'sarray', 'sgraph', 'sketch', 'image']

from . import image
from . import sframe
from . import sarray
from . import sgraph
from . import sketch
