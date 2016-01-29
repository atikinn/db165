-- Needs test08.dsl and test29.dsl to have been executed first.
-- Correctness test: Update values and run a simple query
--
-- UPDATE tbl2 SET col1 = -10 WHERE col1 = -1;
-- UPDATE tbl2 SET col1 = -20 WHERE col2 = -22;
-- UPDATE tbl2 SET col1 = -30 WHERE col1 = -3;
-- UPDATE tbl2 SET col1 = -40 WHERE col3 = -444;
-- UPDATE tbl2 SET col1 = -50 WHERE col1 = -5;
--
-- SELECT col1 FROM tbl2 WHERE col7 > -100 AND col7 < 100000000;
--

s1=select(db1.tbl2.col1,-1)
update(db1.tbl2.col1,s1,-10)

s2=select(db1.tbl2.col1,-22)
update(db1.tbl2.col1,s2,-20)

s3=select(db1.tbl2.col1,-3)
update(db1.tbl2.col1,s3,-30)

s4=select(db1.tbl2.col1,-444)
update(db1.tbl2.col1,s4,-40)

s5=select(db1.tbl2.col1,-5)
update(db1.tbl2.col1,s5,-50)

s6=select(db1.tbl2.col7,-100,100000000)
f1=fetch(db1.tbl2.col1,s6)
tuple(f1)
