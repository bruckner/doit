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

-- Python UDF to convert a string into trigrams.  special handling for start trigram, and word boundaries.
-- Based on algorithm at http://ii.nlm.nih.gov/MTI/trigram.shtml

create or replace function qgrams2(t_in text, q integer) returns setof text as 
$$
# split into lowercase terms
terms = t_in.lower().split()
if len(terms) == 0:
  return

# build a dictionary to hold qgrams
grams = dict()

# iterate through terms
for i in range(0,len(terms)):
  t = terms[i]
  if len(t) == 0:
    continue

  # first qgram of each term is added one extra time with "!" at the end
  firstgram = t[0:q]+'!'
  # update of count is done by popping from dict, incrementing, reinserting
  grams[firstgram] = grams.pop(firstgram, 0)+1
  
  # first letter in each term is added  with "#" at the end
  gram = t[0]+'#'
  grams[gram] = grams.pop(gram, 0)+1
  
  # for each pair of consecutive terms, add a qgram of their first char separated by a space
  if i < len(terms)-1:
    nextterm=terms[i+1]
    gram = t[0]+' '+nextterm[0]
    grams[gram] = grams.pop(gram, 0)+1
    
  # now add standard q-grams for this term
  wlen = len(t)
  for i in range(0, wlen - (q - 1)):
    gram = t[i:i+q]
    grams[gram] = grams.pop(gram, 0)+1

if len(grams.keys()) == 0:
  return

# use yield to return setof
for k in grams:
    for i in range(0,grams[k]):
      yield k
$$ language plpythonu;

