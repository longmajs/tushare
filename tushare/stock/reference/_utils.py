# -*- coding:utf-8 -*-
"""Shared utilities for reference sub-modules."""
from random import randint


def _random(n=13):
    start = 10**(n-1)
    end = (10**n)-1
    return str(randint(start, end))
