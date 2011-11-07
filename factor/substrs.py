# Copyright (c) 2011 Massachusetts Institute of Technology
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import re

# enumerate substrings
def substrs(s):
    ss = set()
    for i in range(0, len(s)):
        for j in range(i+1,len(s)+1):
            ss.add(s[i:j])
    return ss

# enumerate string partitions
def strparts(s):
    pp = []
    if len(s) == 0:
        return [[]]
    for i in range(1, len(s)+1):
        parts = strparts(s[i:])
        for part in parts:
            pp.append([s[:i]] + part)
    return pp

# enumerate prefixes
def prefixes(s):
    return [s[:i] for i in range(1, len(s)+1)]

# enumerate delimited substrings
def dsubstrs(s):
    delims = '(\W)|(_)|(.(?=[\W_]))|([a-z][A-Z])|(\d\D)|(\D\d)'
    delimindexes = [0, len(s)]
    for m in re.finditer(delims, s):
        delimindexes.append(m.start()+1)

    ss = set()
    for i in delimindexes:
        for j in delimindexes:
            if i < j:
                ss.add(s[i:j])
    return ss


print dsubstrs('hello world')
