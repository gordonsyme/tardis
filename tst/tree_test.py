from nose.tools import *

import math

from tardis.tree import Tree


def test_tree_properties():
    t = Tree(4)
    assert_equals(4, t.value)
    assert_equals([], t.children)

    t = Tree(4, [Tree(2), Tree(1)])
    assert_equals(4, t.value)
    assert_equals([Tree(2), Tree(1)], t.children)


def test_fmap_no_children():
    t = Tree(1)
    assert_equals(Tree(2), t.fmap(lambda x: x*2))


def test_fmap():
    t = Tree(1, [ Tree(2, [ Tree(3)
                          , Tree(4)
                          ])
                , Tree(5, [ Tree(6, [ Tree(7)
                                    ])
                          ])
                ])

    expected = Tree(2, [ Tree(4, [ Tree(6)
                                 , Tree(8)
                                 ])
                       , Tree(10, [ Tree(12, [ Tree(14)
                                             ])
                                  ])
                       ])

    assert_equals(expected, t.fmap(lambda x: x*2))


def test_iteration_no_children():
    t = Tree(1)
    assert_equals([1], list(t))


def test_iteration():
    t = Tree(1, [ Tree(2, [ Tree(3)
                          , Tree(4)
                          ])
                , Tree(5, [ Tree(6, [ Tree(7) ]) ])
                ])

    assert_equals([1,2,3,4,5,6,7], list(t))


def test_eq():
    assert_equals(Tree(1), Tree(1))
    assert_not_equals(Tree(2), Tree(1))
    assert_not_equals(Tree(1), 1)
    assert_not_equals(Tree(1), None)

    t1 = Tree(1, [Tree(2)])
    t2 = Tree(1, [Tree(2)])
    assert_equals(t1, t2)

    t1 = Tree(1, [Tree(2)])
    t2 = Tree(1, [Tree(4)])
    assert_not_equals(t1, t2)


def test_build_tree():
    def children_for(x):
        return range(0, x/2)

    expected = Tree(8, [ Tree(0)
                       , Tree(1)
                       , Tree(2, [ Tree(0)
                                 ])
                       , Tree(3, [ Tree(0)
                                 ])
                       ])

    assert_equals(expected, Tree.build_tree(8, children_for))
