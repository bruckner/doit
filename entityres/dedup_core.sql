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

TRUNCATE configuration_properties;
INSERT INTO configuration_properties (name, value) VALUES 
('prob_dist_threshold',0.05), 
('est_dup', 0.00001), 
('bins_count',3), 
('rel_perf_threshold',500), 
('abs_perf_threshold', 0.2), 
('truncate_threshold',0.95),
('question_budget', 50);

--prob_dist_threshold ALIAS FOR $1; -- a ratio for truncating the non-distinguishing attributes, suggested value 0.05
--est_dup  ALIAS FOR $2; -- estimated probability that a pair is duplicates , suggested value 0.002
--bins_count ALIAS FOR $3; -- number of bins per attribute, suggested value 3-5
--rel_perf_threshold ALIAS FOR $4; -- suggested value 1000. Decrease the value to get better recall but worse performance
--abs_perf_threshold ALIAS FOR $5; -- suggested value 0.2. Decrease the value to get better recall but worse performance.



-- End input/output tables
