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

-- input / output table for similarity join and clustering


DROP TABLE IF EXISTS training_clustering CASCADE;
CREATE TABLE training_clustering (local_entity_id integer, global_entity_id integer);


DROP TABLE IF EXISTS global_attrs_types_thr CASCADE;
CREATE TABLE global_attrs_types_thr(tag_id integer, tag_code text, type text, threshold double precision);
CREATE INDEX global_attrs_types_thr__tag_id ON global_attrs_types_thr(tag_id);

-- we don't need the tag_code, but it's here to make it more readable
TRUNCATE global_attrs_types_thr;
INSERT INTO global_attrs_types_thr(tag_id, tag_code, type) values
(1, 'LOC1.FULL_ADDRESS', 'TEXT'),
(2, 'LOC1.ADDRESS', 'TEXT'),
(3, 'LOC1.ADDRESS_2', 'TEXT'),
(4, 'LOC1.CITY', 'TEXT'),
(5, 'LOC1.STATE', 'TEXT'),
(6, 'LOC1.ZIP', 'TEXT'),
(7, 'LOC1.COUNTY', 'TEXT'),
(25, 'CALL_TO_ACTION_URL', 'TEXT'),
(26, 'TITLE', 'TEXT'),
(27, 'DESCRIPTION', 'TEXT'),
(28, 'IMAGE1', 'TEXT'),
(29, 'IMAGE2', 'TEXT'),
(36, 'LOC1.LAT', 'REAL'),
(37, 'LOC1.LON', 'REAL'),
(42, 'PRICE_RANGE', 'TEXT'),
(47, 'PHONE', 'TEXT'),
(48, 'RATING', 'TEXT'),
(55, 'EMAIL', 'TEXT'),
(56, 'WEBSITE', 'TEXT'),
(57, 'HOURS', 'TEXT'),
(58, 'DURATION', 'TEXT'),
(59, 'DIRECTIONS', 'TEXT'),
(70, 'VENUE_NAME', 'TEXT'),
(73, 'REVIEWS', 'TEXT'),
(76, 'TOTAL_NUMBER_OF_RATINGS', 'REAL'),
(null, 'LATITUDE', 'REAL'),
(null, 'LONGITUDE', 'REAL'),
(null, 'LOC1.COUNTRY', 'TEXT'),
(null, 'COUNTY', 'TEXT'),
(null, 'ZIP', 'TEXT'),
(null, 'CITY', 'TEXT');


-- End input/output tables
