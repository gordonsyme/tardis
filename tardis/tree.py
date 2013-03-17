from itertools import chain


class Tree(object):
    def __init__(self, value, children=None):
        if not children:
            children = []

        self._value = value
        self._children = list(children)

    @property
    def value(self):
        return self._value

    @property
    def children(self):
        return list(self._children)

    def fmap(self, f):
        return self.__class__(f(self._value), [child.fmap(f) for child in self._children])

    def __iter__(self):
        """Pre-order iteration"""
        child_values = (value for child in self._children
                              for value in iter(child))

        return chain([self._value], child_values)

    def __eq__(self, other):
        if isinstance(other, Tree):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __ne__(self, other):
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    def __repr__(self):
        return "<Tree({t.value}, {t.children})>".format(t=self)

    @classmethod
    def build_tree(cls, value, children_for):
        children = (cls.build_tree(child, children_for) for child in children_for(value))
        return cls(value, children)
