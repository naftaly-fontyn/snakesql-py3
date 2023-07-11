# -*- coding: utf-8 -*-

str_fun = """

def fun(a, c):
    return a*a + c
"""

l_c = {}
exec(str_fun, locals(), l_c)

print(l_c)


def fun0(a, c):
    return a * a + c

a = l_c['fun'](2, 5)
print(a)