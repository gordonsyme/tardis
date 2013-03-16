from itertools import chain


class Tree(object):
    def __init__(self, value, children=None):
        if not children:
            children = []

        self._value = value
        self._children = children

    @property
    def value(self):
        return self._value

    def __iter__(self):
        """Pre-order iteration"""
        child_values = (value for child in self._children
                              for value in iter(child))

        return chain([self._value], child_values)

    def fmap(self, f):
        return self.__class__(f(self._value), [child.fmap(f) for child in self._children])

    def flatten(self):
        return list(iter(self))

    @classmethod
    def build_tree(cls, value, children_for):
        children = [cls.build_tree(child, children_for) for child in children_for(value)]
        return cls(value, children)
