-- Needs test08.dsl and test29.dsl to have been executed first.
-- Correctness test: Test for updates on columns with index
--
-- SELECT col1 FROM tbl2 WHERE col2 > -100 AND col2 < 100;

s1=select(db1.tbl2.col2,-100,100)
f1=fetch(db1.tbl2.col1,s1)
tuple(f1)
