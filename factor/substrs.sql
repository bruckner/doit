/* Copyright (c) 2011 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

-- UDF for generating list of substrings of a given string
CREATE OR REPLACE FUNCTION substrs (s TEXT) RETURNS SETOF text AS
$$

    ss = set()
    for i in range(0, len(s)):
        for j in range(i+1,len(s)+1):
            ss.add(s[i:j])
    for i in ss:
    	yield i

$$ LANGUAGE plpythonu;

-- UDF for generating a list of delimited substrings of a given string
CREATE OR REPLACE FUNCTION dsubstrs (s TEXT) RETURNS SETOF text AS
$$

    import re
    delims = '(\W)|(_)|(.(?=[\W_]))|([a-z][A-Z])|(\d\D)|(\D\d)'
    delimindexes = [0, len(s)]
    for m in re.finditer(delims, s):
        delimindexes.append(m.start()+1)

    ss = set()
    for i in delimindexes:
        for j in delimindexes:
            if i < j:
                ss.add(s[i:j])
    for i in ss:
    	yield i

$$ LANGUAGE plpythonu;

-- UDF to produce a list of prefixes of a given string
CREATE OR REPLACE FUNCTION prefixes (s TEXT) RETURNS SETOF TEXT AS
$$
    for i in range(0, len(s)):
    	yield s[:i+1]
$$ LANGUAGE plpythonu;


-- This one's pretty specialized, kind of an oddball...
-- UDF to remove a list of strings from a target string
CREATE OR REPLACE FUNCTION remove_substrs (target TEXT, strings_to_remove TEXT) RETURNS TEXT AS
$$
    t = target
    for s in sorted(strings_to_remove.split(','), key=len, reverse=True):
    	if (t.lower().find(s.lower()) > -1):
	    t = t[:t.lower().find(s.lower())] + t[t.lower().find(s.lower()) + len(s):]
    return t
$$ LANGUAGE plpythonu; 
