-- Needs test25.dsl to have been executed first.
-- Testing for correctness - simple hashjoin between two tables. The columns based on which we join are also
-- filtered upon.
--
-- Query in SQL:
-- SELECT tbl4.col1, tbl5.col1
-- FROM tbl4, tbl5
-- WHERE tbl4.col1 = tbl5.col1
-- AND tbl4.col1 >= 20000 AND tbl4.col1 < 40000
-- AND tbl5.col1 >= 30000 AND tbl5.col1 < 70000;

s1=select(db1.tbl4.col1,20000,40000)
f1=fetch(db1.tbl4.col1,s1)

s2=select(db1.tbl5.col1,30000,70000)
f2=fetch(db1.tbl5.col1,s2)

s3,s4=hashjoin(f1,s1,f2,s2)

f3=fetch(db1.tbl4.col1,s3)
f4=fetch(db1.tbl5.col1,s4)

tuple(f3,f4)

