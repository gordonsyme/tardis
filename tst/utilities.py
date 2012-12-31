# From http://ludios.org/testing-your-eq-ne-cmp/

from nose.tools import *

def assert_really_equal(a, b):
    # assertEqual first, because it will have a good message if the assertion
    # fails.
    assert_equal(a, b)
    assert_equal(b, a)
    assert_true(a == b)
    assert_true(b == a)
    assert_false(a != b)
    assert_false(b != a)
    assert_equal(0, cmp(a, b))
    assert_equal(0, cmp(b, a))

def assert_really_not_equal(a, b):
    # assert_not_equal first, because it will have a good message if the
    # assert_ion fails.
    assert_not_equal(a, b)
    assert_not_equal(b, a)
    assert_false(a == b)
    assert_false(b == a)
    assert_true(a != b)
    assert_true(b != a)
    assert_not_equal(0, cmp(a, b))
    assert_not_equal(0, cmp(b, a))
