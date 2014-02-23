-- [DELTA] 
--    Range: 60-120
--    Default: 90
CREATE TABLE LINEITEM (
        orderkey       INT,
        partkey        INT,
        suppkey        INT,
        linenumber     INT,
        quantity       DECIMAL,
        extendedprice  DECIMAL,
        discount       DECIMAL,
        tax            DECIMAL,
        returnflag     CHAR(1),
        linestatus     CHAR(1),
        shipdate       DATE,
        commitdate     DATE,
        receiptdate    DATE,
        shipinstruct   CHAR(25),
        shipmode       CHAR(10),
        comment        VARCHAR(44)
    );
SELECT
  returnflag,
  linestatus,
  shipdate
FROM
  lineitem
WHERE
  shipdate <= DATE('1998-09-01'); -- (- interval '[DELTA]' day (3));
