"""Test the hyparviewpy.util module."""

import asyncio
import inspect
import io
import random
import unittest

from hyparviewpy import util
from .tools import async_test


class Test_Util(unittest.TestCase):
    def test_log(self):
        """Test the debug logger."""
        f = io.StringIO('')
        log = util.Log(f, False)
        util._out_fh = f
        log.write('hello world')
        self.assertEqual(f.getvalue(), '')
        log = util.Log(f, True)
        log.write('hello world')
        self.assertEqual(f.getvalue(), 'hello world')

    def test_unpack_data(self):
        """Test the util.unpack_data function."""
        self.assertEqual((None, None), util.unpack_data(''))
        self.assertEqual(('a', None), util.unpack_data('a'))
        self.assertEqual(('a', ''), util.unpack_data('a '))
        self.assertEqual(('a', 'b'), util.unpack_data('a b'))
        self.assertEqual(('a', 'b c'), util.unpack_data('a b c'))

    def test_unpack_nid(self):
        """Test the util.unpack_nid function."""
        self.assertEqual((None, None, None), util.unpack_nid(''))
        self.assertEqual((None, None, None), util.unpack_nid('a'))
        self.assertEqual((None, None, None), util.unpack_nid('a@b'))
        self.assertEqual((None, None, None), util.unpack_nid('a:b'))
        self.assertEqual((None, None, None), util.unpack_nid('a:b@c'))
        self.assertEqual(('a', 'b', 'c'), util.unpack_nid('a@b:c'))
        self.assertEqual(('a@', 'b:', 'c'), util.unpack_nid('a@@b::c'))
        self.assertEqual(('a@b:c', 'd', 'e'), util.unpack_nid('a@b:c@d:e'))

    def test_sample(self):
        """Test the util.sample_set function."""
        self.assertEqual([], util.sample_set(set(), 1))
        self.assertEqual([], util.sample_set({1}, 0))

        self.assertEqual([1], util.sample_set({1}, 1))
        s = {1}
        self.assertEqual([1], util.sample_set(s, 1, True))
        self.assertEqual(set(), s)

        self.assertEqual({1, 2}, set(util.sample_set({1, 2}, 2)))
        s = {1, 2}
        self.assertEqual({1, 2}, set(util.sample_set(s, 2, True)))
        self.assertEqual(set(), s)

        s = set()
        while len(s) < 20:
            s.add(random.randint(0, 100))
        s_copy = s.copy()

        # get a sample without removal and check original is a superset
        lst = util.sample_set(s, 10)
        self.assertEqual(len(lst), 10)
        self.assertEqual(s, s_copy)
        self.assertTrue(s.issuperset(set(lst)))

        # get a sample with removal and check result is disjoint with set and
        # union is equal to original
        lst = util.sample_set(s, 10, True)
        self.assertEqual(len(lst), 10)
        self.assertEqual(len(s), 10)
        self.assertTrue(s.isdisjoint(set(lst)))
        self.assertEqual(s_copy, s.union(set(lst)))

    @async_test()
    async def test_callback(self):
        """Test the util.run_callback function."""
        def fun(lst):
            lst.append(0)

        async def afun(lst):
            await asyncio.sleep(0.1)
            lst.append(1)

        lst = []
        self.assertIsNone(util.run_callback(fun, lst))
        self.assertEqual(lst, [0])

        t = util.run_callback(afun, lst)
        self.assertTrue(inspect.isawaitable(t))
        await t
        self.assertEqual(lst, [0, 1])
