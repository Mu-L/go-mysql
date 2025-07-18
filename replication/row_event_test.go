package replication

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestDecodeDecimal(t *testing.T) {
	// _PLACEHOLDER_ := 0
	testcases := []struct {
		Data        []byte
		Precision   int
		Decimals    int
		Expected    string
		ExpectedPos int
		ExpectedErr error
	}{
		// These are cases from the mysql test cases
		/*
			-- Generated with gentestsql.go --
			DROP TABLE IF EXISTS decodedecimal;
			CREATE TABLE decodedecimal (
			    id     int(11) not null auto_increment,
			    v4_2 decimal(4,2),
			    v5_0 decimal(5,0),
			    v7_3 decimal(7,3),
			    v10_2 decimal(10,2),
			    v10_3 decimal(10,3),
			    v13_2 decimal(13,2),
			    v15_14 decimal(15,14),
			    v20_10 decimal(20,10),
			    v30_5 decimal(30,5),
			    v30_20 decimal(30,20),
			    v30_25 decimal(30,25),
			    prec   int(11),
			    scale  int(11),
			    PRIMARY KEY(id)
			) engine=InnoDB;
			INSERT INTO decodedecimal (v4_2,v5_0,v7_3,v10_2,v10_3,v13_2,v15_14,v20_10,v30_5,v30_20,v30_25,prec,scale) VALUES
			("-10.55","-10.55","-10.55","-10.55","-10.55","-10.55","-10.55","-10.55","-10.55","-10.55","-10.55",4,2),
			("0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345","0.0123456789012345678912345",30,25),
			("12345","12345","12345","12345","12345","12345","12345","12345","12345","12345","12345",5,0),
			("12345","12345","12345","12345","12345","12345","12345","12345","12345","12345","12345",10,3),
			("123.45","123.45","123.45","123.45","123.45","123.45","123.45","123.45","123.45","123.45","123.45",10,3),
			("-123.45","-123.45","-123.45","-123.45","-123.45","-123.45","-123.45","-123.45","-123.45","-123.45","-123.45",20,10),
			(".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",15,14),
			(".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",".00012345000098765",22,20),
			(".12345000098765",".12345000098765",".12345000098765",".12345000098765",".12345000098765",".12345000098765",".12345000098765",".12345000098765",".12345000098765",".12345000098765",".12345000098765",30,20),
			("-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765","-.000000012345000098765",30,20),
			("1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5","1234500009876.5",30,5),
			("111111111.11","111111111.11","111111111.11","111111111.11","111111111.11","111111111.11","111111111.11","111111111.11","111111111.11","111111111.11","111111111.11",10,2),
			("000000000.01","000000000.01","000000000.01","000000000.01","000000000.01","000000000.01","000000000.01","000000000.01","000000000.01","000000000.01","000000000.01",7,3),
			("123.4","123.4","123.4","123.4","123.4","123.4","123.4","123.4","123.4","123.4","123.4",10,2),
			("-562.58","-562.58","-562.58","-562.58","-562.58","-562.58","-562.58","-562.58","-562.58","-562.58","-562.58",13,2),
			("-3699.01","-3699.01","-3699.01","-3699.01","-3699.01","-3699.01","-3699.01","-3699.01","-3699.01","-3699.01","-3699.01",13,2),
			("-1948.14","-1948.14","-1948.14","-1948.14","-1948.14","-1948.14","-1948.14","-1948.14","-1948.14","-1948.14","-1948.14",13,2)
			;
			select * from decodedecimal;
			+----+--------+-------+-----------+-------------+-------------+----------------+-------------------+-----------------------+---------------------+---------------------------------+---------------------------------+------+-------+
			| id | v4_2   | v5_0  | v7_3      | v10_2       | v10_3       | v13_2          | v15_14            | v20_10                | v30_5               | v30_20                          | v30_25                          | prec | scale |
			+----+--------+-------+-----------+-------------+-------------+----------------+-------------------+-----------------------+---------------------+---------------------------------+---------------------------------+------+-------+
			|  1 | -10.55 |   -11 |   -10.550 |      -10.55 |     -10.550 |         -10.55 | -9.99999999999999 |        -10.5500000000 |           -10.55000 |        -10.55000000000000000000 |   -10.5500000000000000000000000 |    4 |     2 |
			|  2 |   0.01 |     0 |     0.012 |        0.01 |       0.012 |           0.01 |  0.01234567890123 |          0.0123456789 |             0.01235 |          0.01234567890123456789 |     0.0123456789012345678912345 |   30 |    25 |
			|  3 |  99.99 | 12345 |  9999.999 |    12345.00 |   12345.000 |       12345.00 |  9.99999999999999 |      12345.0000000000 |         12345.00000 |      12345.00000000000000000000 | 12345.0000000000000000000000000 |    5 |     0 |
			|  4 |  99.99 | 12345 |  9999.999 |    12345.00 |   12345.000 |       12345.00 |  9.99999999999999 |      12345.0000000000 |         12345.00000 |      12345.00000000000000000000 | 12345.0000000000000000000000000 |   10 |     3 |
			|  5 |  99.99 |   123 |   123.450 |      123.45 |     123.450 |         123.45 |  9.99999999999999 |        123.4500000000 |           123.45000 |        123.45000000000000000000 |   123.4500000000000000000000000 |   10 |     3 |
			|  6 | -99.99 |  -123 |  -123.450 |     -123.45 |    -123.450 |        -123.45 | -9.99999999999999 |       -123.4500000000 |          -123.45000 |       -123.45000000000000000000 |  -123.4500000000000000000000000 |   20 |    10 |
			|  7 |   0.00 |     0 |     0.000 |        0.00 |       0.000 |           0.00 |  0.00012345000099 |          0.0001234500 |             0.00012 |          0.00012345000098765000 |     0.0001234500009876500000000 |   15 |    14 |
			|  8 |   0.00 |     0 |     0.000 |        0.00 |       0.000 |           0.00 |  0.00012345000099 |          0.0001234500 |             0.00012 |          0.00012345000098765000 |     0.0001234500009876500000000 |   22 |    20 |
			|  9 |   0.12 |     0 |     0.123 |        0.12 |       0.123 |           0.12 |  0.12345000098765 |          0.1234500010 |             0.12345 |          0.12345000098765000000 |     0.1234500009876500000000000 |   30 |    20 |
			| 10 |   0.00 |     0 |     0.000 |        0.00 |       0.000 |           0.00 | -0.00000001234500 |         -0.0000000123 |             0.00000 |         -0.00000001234500009877 |    -0.0000000123450000987650000 |   30 |    20 |
			| 11 |  99.99 | 99999 |  9999.999 | 99999999.99 | 9999999.999 | 99999999999.99 |  9.99999999999999 | 9999999999.9999999999 | 1234500009876.50000 | 9999999999.99999999999999999999 | 99999.9999999999999999999999999 |   30 |     5 |
			| 12 |  99.99 | 99999 |  9999.999 | 99999999.99 | 9999999.999 |   111111111.11 |  9.99999999999999 |  111111111.1100000000 |     111111111.11000 |  111111111.11000000000000000000 | 99999.9999999999999999999999999 |   10 |     2 |
			| 13 |   0.01 |     0 |     0.010 |        0.01 |       0.010 |           0.01 |  0.01000000000000 |          0.0100000000 |             0.01000 |          0.01000000000000000000 |     0.0100000000000000000000000 |    7 |     3 |
			| 14 |  99.99 |   123 |   123.400 |      123.40 |     123.400 |         123.40 |  9.99999999999999 |        123.4000000000 |           123.40000 |        123.40000000000000000000 |   123.4000000000000000000000000 |   10 |     2 |
			| 15 | -99.99 |  -563 |  -562.580 |     -562.58 |    -562.580 |        -562.58 | -9.99999999999999 |       -562.5800000000 |          -562.58000 |       -562.58000000000000000000 |  -562.5800000000000000000000000 |   13 |     2 |
			| 16 | -99.99 | -3699 | -3699.010 |    -3699.01 |   -3699.010 |       -3699.01 | -9.99999999999999 |      -3699.0100000000 |         -3699.01000 |      -3699.01000000000000000000 | -3699.0100000000000000000000000 |   13 |     2 |
			| 17 | -99.99 | -1948 | -1948.140 |    -1948.14 |   -1948.140 |       -1948.14 | -9.99999999999999 |      -1948.1400000000 |         -1948.14000 |      -1948.14000000000000000000 | -1948.1400000000000000000000000 |   13 |     2 |
			+----+--------+-------+-----------+-------------+-------------+----------------+-------------------+-----------------------+---------------------+---------------------------------+---------------------------------+------+-------+
		*/
		{[]byte{117, 200, 127, 255}, 4, 2, "-10.55", 2, nil},
		{[]byte{127, 255, 244, 127, 245}, 5, 0, "-11", 3, nil},
		{[]byte{127, 245, 253, 217, 127, 255}, 7, 3, "-10.550", 4, nil},
		{[]byte{127, 255, 255, 245, 200, 127, 255}, 10, 2, "-10.55", 5, nil},
		{[]byte{127, 255, 255, 245, 253, 217, 127, 255}, 10, 3, "-10.550", 6, nil},
		{[]byte{127, 255, 255, 255, 245, 200, 118, 196}, 13, 2, "-10.55", 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, "-9.99999999999999", 8, nil},
		{[]byte{127, 255, 255, 255, 245, 223, 55, 170, 127, 255, 127, 255}, 20, 10, "-10.5500000000", 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 245, 255, 41, 39, 127, 255}, 30, 5, "-10.55000", 15, nil},
		{[]byte{127, 255, 255, 255, 245, 223, 55, 170, 127, 255, 255, 255, 255, 255, 127, 255}, 30, 20, "-10.55000000000000000000", 14, nil},
		{[]byte{127, 255, 245, 223, 55, 170, 127, 255, 255, 255, 255, 255, 255, 255, 255, 4, 0}, 30, 25, "-10.5500000000000000000000000", 15, nil},
		{[]byte{128, 1, 128, 0}, 4, 2, "0.01", 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, "0", 3, nil},
		{[]byte{128, 0, 0, 12, 128, 0}, 7, 3, "0.012", 4, nil},
		{[]byte{128, 0, 0, 0, 1, 128, 0}, 10, 2, "0.01", 5, nil},
		{[]byte{128, 0, 0, 0, 0, 12, 128, 0}, 10, 3, "0.012", 6, nil},
		{[]byte{128, 0, 0, 0, 0, 1, 128, 0}, 13, 2, "0.01", 6, nil},
		{[]byte{128, 0, 188, 97, 78, 1, 96, 11, 128, 0}, 15, 14, "0.01234567890123", 8, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 188, 97, 78, 9, 128, 0}, 20, 10, "0.0123456789", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 211, 128, 0}, 30, 5, "0.01235", 15, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 188, 97, 78, 53, 183, 191, 135, 89, 128, 0}, 30, 20, "0.01234567890123456789", 14, nil},
		{[]byte{128, 0, 0, 0, 188, 97, 78, 53, 183, 191, 135, 0, 135, 253, 217, 30, 0}, 30, 25, "0.0123456789012345678912345", 15, nil},
		{[]byte{227, 99, 128, 48}, 4, 2, "99.99", 2, nil},
		{[]byte{128, 48, 57, 167, 15}, 5, 0, "12345", 3, nil},
		{[]byte{167, 15, 3, 231, 128, 0}, 7, 3, "9999.999", 4, nil},
		{[]byte{128, 0, 48, 57, 0, 128, 0}, 10, 2, "12345.00", 5, nil},
		{[]byte{128, 0, 48, 57, 0, 0, 128, 0}, 10, 3, "12345.000", 6, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 137, 59}, 13, 2, "12345.00", 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0}, 15, 14, "9.99999999999999", 8, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 128, 0}, 20, 10, "12345.0000000000", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 57, 0, 0, 0, 128, 0}, 30, 5, "12345.00000", 15, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 48}, 30, 20, "12345.00000000000000000000", 14, nil},
		{[]byte{128, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0}, 30, 25, "12345.0000000000000000000000000", 15, nil},
		{[]byte{227, 99, 128, 48}, 4, 2, "99.99", 2, nil},
		{[]byte{128, 48, 57, 167, 15}, 5, 0, "12345", 3, nil},
		{[]byte{167, 15, 3, 231, 128, 0}, 7, 3, "9999.999", 4, nil},
		{[]byte{128, 0, 48, 57, 0, 128, 0}, 10, 2, "12345.00", 5, nil},
		{[]byte{128, 0, 48, 57, 0, 0, 128, 0}, 10, 3, "12345.000", 6, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 137, 59}, 13, 2, "12345.00", 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0}, 15, 14, "9.99999999999999", 8, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 128, 0}, 20, 10, "12345.0000000000", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 57, 0, 0, 0, 128, 0}, 30, 5, "12345.00000", 15, nil},
		{[]byte{128, 0, 0, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 48}, 30, 20, "12345.00000000000000000000", 14, nil},
		{[]byte{128, 48, 57, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0}, 30, 25, "12345.0000000000000000000000000", 15, nil},
		{[]byte{227, 99, 128, 0}, 4, 2, "99.99", 2, nil},
		{[]byte{128, 0, 123, 128, 123}, 5, 0, "123", 3, nil},
		{[]byte{128, 123, 1, 194, 128, 0}, 7, 3, "123.450", 4, nil},
		{[]byte{128, 0, 0, 123, 45, 128, 0}, 10, 2, "123.45", 5, nil},
		{[]byte{128, 0, 0, 123, 1, 194, 128, 0}, 10, 3, "123.450", 6, nil},
		{[]byte{128, 0, 0, 0, 123, 45, 137, 59}, 13, 2, "123.45", 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0}, 15, 14, "9.99999999999999", 8, nil},
		{[]byte{128, 0, 0, 0, 123, 26, 210, 116, 128, 0, 128, 0}, 20, 10, "123.4500000000", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 123, 0, 175, 200, 128, 0}, 30, 5, "123.45000", 15, nil},
		{[]byte{128, 0, 0, 0, 123, 26, 210, 116, 128, 0, 0, 0, 0, 0, 128, 0}, 30, 20, "123.45000000000000000000", 14, nil},
		{[]byte{128, 0, 123, 26, 210, 116, 128, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0}, 30, 25, "123.4500000000000000000000000", 15, nil},
		{[]byte{28, 156, 127, 255}, 4, 2, "-99.99", 2, nil},
		{[]byte{127, 255, 132, 127, 132}, 5, 0, "-123", 3, nil},
		{[]byte{127, 132, 254, 61, 127, 255}, 7, 3, "-123.450", 4, nil},
		{[]byte{127, 255, 255, 132, 210, 127, 255}, 10, 2, "-123.45", 5, nil},
		{[]byte{127, 255, 255, 132, 254, 61, 127, 255}, 10, 3, "-123.450", 6, nil},
		{[]byte{127, 255, 255, 255, 132, 210, 118, 196}, 13, 2, "-123.45", 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, "-9.99999999999999", 8, nil},
		{[]byte{127, 255, 255, 255, 132, 229, 45, 139, 127, 255, 127, 255}, 20, 10, "-123.4500000000", 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 132, 255, 80, 55, 127, 255}, 30, 5, "-123.45000", 15, nil},
		{[]byte{127, 255, 255, 255, 132, 229, 45, 139, 127, 255, 255, 255, 255, 255, 127, 255}, 30, 20, "-123.45000000000000000000", 14, nil},
		{[]byte{127, 255, 132, 229, 45, 139, 127, 255, 255, 255, 255, 255, 255, 255, 255, 20, 0}, 30, 25, "-123.4500000000000000000000000", 15, nil},
		{[]byte{128, 0, 128, 0}, 4, 2, "0.00", 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, "0", 3, nil},
		{[]byte{128, 0, 0, 0, 128, 0}, 7, 3, "0.000", 4, nil},
		{[]byte{128, 0, 0, 0, 0, 128, 0}, 10, 2, "0.00", 5, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 10, 3, "0.000", 6, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 13, 2, "0.00", 6, nil},
		{[]byte{128, 0, 1, 226, 58, 0, 0, 99, 128, 0}, 15, 14, "0.00012345000099", 8, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 128, 0}, 20, 10, "0.0001234500", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 128, 0}, 30, 5, "0.00012", 15, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 128, 0}, 30, 20, "0.00012345000098765000", 14, nil},
		{[]byte{128, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 0, 0, 0, 15, 0}, 30, 25, "0.0001234500009876500000000", 15, nil},
		{[]byte{128, 0, 128, 0}, 4, 2, "0.00", 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, "0", 3, nil},
		{[]byte{128, 0, 0, 0, 128, 0}, 7, 3, "0.000", 4, nil},
		{[]byte{128, 0, 0, 0, 0, 128, 0}, 10, 2, "0.00", 5, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 10, 3, "0.000", 6, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 13, 2, "0.00", 6, nil},
		{[]byte{128, 0, 1, 226, 58, 0, 0, 99, 128, 0}, 15, 14, "0.00012345000099", 8, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 128, 0}, 20, 10, "0.0001234500", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 128, 0}, 30, 5, "0.00012", 15, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 128, 0}, 30, 20, "0.00012345000098765000", 14, nil},
		{[]byte{128, 0, 0, 0, 1, 226, 58, 0, 15, 18, 2, 0, 0, 0, 0, 22, 0}, 30, 25, "0.0001234500009876500000000", 15, nil},
		{[]byte{128, 12, 128, 0}, 4, 2, "0.12", 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, "0", 3, nil},
		{[]byte{128, 0, 0, 123, 128, 0}, 7, 3, "0.123", 4, nil},
		{[]byte{128, 0, 0, 0, 12, 128, 0}, 10, 2, "0.12", 5, nil},
		{[]byte{128, 0, 0, 0, 0, 123, 128, 0}, 10, 3, "0.123", 6, nil},
		{[]byte{128, 0, 0, 0, 0, 12, 128, 7}, 13, 2, "0.12", 6, nil},
		{[]byte{128, 7, 91, 178, 144, 1, 129, 205, 128, 0}, 15, 14, "0.12345000098765", 8, nil},
		{[]byte{128, 0, 0, 0, 0, 7, 91, 178, 145, 0, 128, 0}, 20, 10, "0.1234500010", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 57, 128, 0}, 30, 5, "0.12345", 15, nil},
		{[]byte{128, 0, 0, 0, 0, 7, 91, 178, 144, 58, 222, 87, 208, 0, 128, 0}, 30, 20, "0.12345000098765000000", 14, nil},
		{[]byte{128, 0, 0, 7, 91, 178, 144, 58, 222, 87, 208, 0, 0, 0, 0, 30, 0}, 30, 25, "0.1234500009876500000000000", 15, nil},
		{[]byte{128, 0, 128, 0}, 4, 2, "0.00", 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, "0", 3, nil},
		{[]byte{128, 0, 0, 0, 128, 0}, 7, 3, "0.000", 4, nil},
		{[]byte{128, 0, 0, 0, 0, 128, 0}, 10, 2, "0.00", 5, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 128, 0}, 10, 3, "0.000", 6, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 127, 255}, 13, 2, "0.00", 6, nil},
		{[]byte{127, 255, 255, 255, 243, 255, 121, 59, 127, 255}, 15, 14, "-0.00000001234500", 8, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 243, 252, 128, 0}, 20, 10, "-0.0000000123", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 127, 255}, 30, 5, "0.00000", 15, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 243, 235, 111, 183, 93, 178, 127, 255}, 30, 20, "-0.00000001234500009877", 14, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 243, 235, 111, 183, 93, 255, 139, 69, 47, 30, 0}, 30, 25, "-0.0000000123450000987650000", 15, nil},
		{[]byte{227, 99, 129, 134}, 4, 2, "99.99", 2, nil},
		{[]byte{129, 134, 159, 167, 15}, 5, 0, "99999", 3, nil},
		{[]byte{167, 15, 3, 231, 133, 245}, 7, 3, "9999.999", 4, nil},
		{[]byte{133, 245, 224, 255, 99, 128, 152}, 10, 2, "99999999.99", 5, nil},
		{[]byte{128, 152, 150, 127, 3, 231, 227, 59}, 10, 3, "9999999.999", 6, nil},
		{[]byte{227, 59, 154, 201, 255, 99, 137, 59}, 13, 2, "99999999999.99", 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 137, 59}, 15, 14, "9.99999999999999", 8, nil},
		{[]byte{137, 59, 154, 201, 255, 59, 154, 201, 255, 9, 128, 0}, 20, 10, "9999999999.9999999999", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 4, 210, 29, 205, 139, 148, 0, 195, 80, 137, 59}, 30, 5, "1234500009876.50000", 15, nil},
		{[]byte{137, 59, 154, 201, 255, 59, 154, 201, 255, 59, 154, 201, 255, 99, 129, 134}, 30, 20, "9999999999.99999999999999999999", 14, nil},
		{[]byte{129, 134, 159, 59, 154, 201, 255, 59, 154, 201, 255, 0, 152, 150, 127, 30, 0}, 30, 25, "99999.9999999999999999999999999", 15, nil},
		{[]byte{227, 99, 129, 134}, 4, 2, "99.99", 2, nil},
		{[]byte{129, 134, 159, 167, 15}, 5, 0, "99999", 3, nil},
		{[]byte{167, 15, 3, 231, 133, 245}, 7, 3, "9999.999", 4, nil},
		{[]byte{133, 245, 224, 255, 99, 128, 152}, 10, 2, "99999999.99", 5, nil},
		{[]byte{128, 152, 150, 127, 3, 231, 128, 6}, 10, 3, "9999999.999", 6, nil},
		{[]byte{128, 6, 159, 107, 199, 11, 137, 59}, 13, 2, "111111111.11", 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 6}, 15, 14, "9.99999999999999", 8, nil},
		{[]byte{128, 6, 159, 107, 199, 6, 142, 119, 128, 0, 128, 0}, 20, 10, "111111111.1100000000", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 6, 159, 107, 199, 0, 42, 248, 128, 6}, 30, 5, "111111111.11000", 15, nil},
		{[]byte{128, 6, 159, 107, 199, 6, 142, 119, 128, 0, 0, 0, 0, 0, 129, 134}, 30, 20, "111111111.11000000000000000000", 14, nil},
		{[]byte{129, 134, 159, 59, 154, 201, 255, 59, 154, 201, 255, 0, 152, 150, 127, 10, 0}, 30, 25, "99999.9999999999999999999999999", 15, nil},
		{[]byte{128, 1, 128, 0}, 4, 2, "0.01", 2, nil},
		{[]byte{128, 0, 0, 128, 0}, 5, 0, "0", 3, nil},
		{[]byte{128, 0, 0, 10, 128, 0}, 7, 3, "0.010", 4, nil},
		{[]byte{128, 0, 0, 0, 1, 128, 0}, 10, 2, "0.01", 5, nil},
		{[]byte{128, 0, 0, 0, 0, 10, 128, 0}, 10, 3, "0.010", 6, nil},
		{[]byte{128, 0, 0, 0, 0, 1, 128, 0}, 13, 2, "0.01", 6, nil},
		{[]byte{128, 0, 152, 150, 128, 0, 0, 0, 128, 0}, 15, 14, "0.01000000000000", 8, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 152, 150, 128, 0, 128, 0}, 20, 10, "0.0100000000", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 232, 128, 0}, 30, 5, "0.01000", 15, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 152, 150, 128, 0, 0, 0, 0, 0, 128, 0}, 30, 20, "0.01000000000000000000", 14, nil},
		{[]byte{128, 0, 0, 0, 152, 150, 128, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0}, 30, 25, "0.0100000000000000000000000", 15, nil},
		{[]byte{227, 99, 128, 0}, 4, 2, "99.99", 2, nil},
		{[]byte{128, 0, 123, 128, 123}, 5, 0, "123", 3, nil},
		{[]byte{128, 123, 1, 144, 128, 0}, 7, 3, "123.400", 4, nil},
		{[]byte{128, 0, 0, 123, 40, 128, 0}, 10, 2, "123.40", 5, nil},
		{[]byte{128, 0, 0, 123, 1, 144, 128, 0}, 10, 3, "123.400", 6, nil},
		{[]byte{128, 0, 0, 0, 123, 40, 137, 59}, 13, 2, "123.40", 6, nil},
		{[]byte{137, 59, 154, 201, 255, 1, 134, 159, 128, 0}, 15, 14, "9.99999999999999", 8, nil},
		{[]byte{128, 0, 0, 0, 123, 23, 215, 132, 0, 0, 128, 0}, 20, 10, "123.4000000000", 10, nil},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 123, 0, 156, 64, 128, 0}, 30, 5, "123.40000", 15, nil},
		{[]byte{128, 0, 0, 0, 123, 23, 215, 132, 0, 0, 0, 0, 0, 0, 128, 0}, 30, 20, "123.40000000000000000000", 14, nil},
		{[]byte{128, 0, 123, 23, 215, 132, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0}, 30, 25, "123.4000000000000000000000000", 15, nil},
		{[]byte{28, 156, 127, 253}, 4, 2, "-99.99", 2, nil},
		{[]byte{127, 253, 204, 125, 205}, 5, 0, "-563", 3, nil},
		{[]byte{125, 205, 253, 187, 127, 255}, 7, 3, "-562.580", 4, nil},
		{[]byte{127, 255, 253, 205, 197, 127, 255}, 10, 2, "-562.58", 5, nil},
		{[]byte{127, 255, 253, 205, 253, 187, 127, 255}, 10, 3, "-562.580", 6, nil},
		{[]byte{127, 255, 255, 253, 205, 197, 118, 196}, 13, 2, "-562.58", 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, "-9.99999999999999", 8, nil},
		{[]byte{127, 255, 255, 253, 205, 221, 109, 230, 255, 255, 127, 255}, 20, 10, "-562.5800000000", 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 253, 205, 255, 29, 111, 127, 255}, 30, 5, "-562.58000", 15, nil},
		{[]byte{127, 255, 255, 253, 205, 221, 109, 230, 255, 255, 255, 255, 255, 255, 127, 253}, 30, 20, "-562.58000000000000000000", 14, nil},
		{[]byte{127, 253, 205, 221, 109, 230, 255, 255, 255, 255, 255, 255, 255, 255, 255, 13, 0}, 30, 25, "-562.5800000000000000000000000", 15, nil},
		{[]byte{28, 156, 127, 241}, 4, 2, "-99.99", 2, nil},
		{[]byte{127, 241, 140, 113, 140}, 5, 0, "-3699", 3, nil},
		{[]byte{113, 140, 255, 245, 127, 255}, 7, 3, "-3699.010", 4, nil},
		{[]byte{127, 255, 241, 140, 254, 127, 255}, 10, 2, "-3699.01", 5, nil},
		{[]byte{127, 255, 241, 140, 255, 245, 127, 255}, 10, 3, "-3699.010", 6, nil},
		{[]byte{127, 255, 255, 241, 140, 254, 118, 196}, 13, 2, "-3699.01", 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, "-9.99999999999999", 8, nil},
		{[]byte{127, 255, 255, 241, 140, 255, 103, 105, 127, 255, 127, 255}, 20, 10, "-3699.0100000000", 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 241, 140, 255, 252, 23, 127, 255}, 30, 5, "-3699.01000", 15, nil},
		{[]byte{127, 255, 255, 241, 140, 255, 103, 105, 127, 255, 255, 255, 255, 255, 127, 241}, 30, 20, "-3699.01000000000000000000", 14, nil},
		{[]byte{127, 241, 140, 255, 103, 105, 127, 255, 255, 255, 255, 255, 255, 255, 255, 13, 0}, 30, 25, "-3699.0100000000000000000000000", 15, nil},
		{[]byte{28, 156, 127, 248}, 4, 2, "-99.99", 2, nil},
		{[]byte{127, 248, 99, 120, 99}, 5, 0, "-1948", 3, nil},
		{[]byte{120, 99, 255, 115, 127, 255}, 7, 3, "-1948.140", 4, nil},
		{[]byte{127, 255, 248, 99, 241, 127, 255}, 10, 2, "-1948.14", 5, nil},
		{[]byte{127, 255, 248, 99, 255, 115, 127, 255}, 10, 3, "-1948.140", 6, nil},
		{[]byte{127, 255, 255, 248, 99, 241, 118, 196}, 13, 2, "-1948.14", 6, nil},
		{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, 15, 14, "-9.99999999999999", 8, nil},
		{[]byte{127, 255, 255, 248, 99, 247, 167, 196, 255, 255, 127, 255}, 20, 10, "-1948.1400000000", 10, nil},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 248, 99, 255, 201, 79, 127, 255}, 30, 5, "-1948.14000", 15, nil},
		{[]byte{127, 255, 255, 248, 99, 247, 167, 196, 255, 255, 255, 255, 255, 255, 127, 248}, 30, 20, "-1948.14000000000000000000", 14, nil},
		{[]byte{127, 248, 99, 247, 167, 196, 255, 255, 255, 255, 255, 255, 255, 255, 255, 13, 0}, 30, 25, "-1948.1400000000000000000000000", 15, nil},
	}
	for _, tc := range testcases {
		value, pos, err := decodeDecimal(tc.Data, tc.Precision, tc.Decimals, false)
		require.Equal(t, tc.Expected, value.(string))
		require.Equal(t, tc.ExpectedPos, pos)
		require.Equal(t, tc.ExpectedErr, err)

		value, pos, err = decodeDecimal(tc.Data, tc.Precision, tc.Decimals, true)
		expectedDecimal, _ := decimal.NewFromString(tc.Expected)
		require.True(t, expectedDecimal.Equal(value.(decimal.Decimal)))
		require.Equal(t, tc.ExpectedPos, pos)
		require.Equal(t, tc.ExpectedErr, err)
	}
}

func TestLastNull(t *testing.T) {
	// Table format:
	// desc funnytable;
	// +-------+------------+------+-----+---------+-------+
	// | Field | Type       | Null | Key | Default | Extra |
	// +-------+------------+------+-----+---------+-------+
	// | value | tinyint(4) | YES  |     | NULL    |       |
	// +-------+------------+------+-----+---------+-------+

	// insert into funnytable values (1), (2), (null);
	// insert into funnytable values (1), (null), (2);
	// all must get 3 rows

	tableMapEventData := []byte("\xd3\x01\x00\x00\x00\x00\x01\x00\x04test\x00\nfunnytable\x00\x01\x01\x00\x01")

	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	require.NoError(t, err)

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	tbls := [][]byte{
		[]byte("\xd3\x01\x00\x00\x00\x00\x01\x00\x02\x00\x01\xff\xfe\x01\xff\xfe\x02"),
		[]byte("\xd3\x01\x00\x00\x00\x00\x01\x00\x02\x00\x01\xff\xfe\x01\xfe\x02\xff"),
	}

	for _, tbl := range tbls {
		rows.Rows = nil
		err = rows.Decode(tbl)
		require.NoError(t, err)
		require.Len(t, rows.Rows, 3)
	}
}

func TestParseRowPanic(t *testing.T) {
	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	tableMapEvent.TableID = 1810
	tableMapEvent.ColumnType = []byte{3, 15, 15, 15, 9, 15, 15, 252, 3, 3, 3, 15, 3, 3, 3, 15, 3, 15, 1, 15, 3, 1, 252, 15, 15, 15}
	tableMapEvent.ColumnMeta = []uint16{0, 108, 60, 765, 0, 765, 765, 4, 0, 0, 0, 765, 0, 0, 0, 3, 0, 3, 0, 765, 0, 0, 2, 108, 108, 108}

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	data := []byte{18, 7, 0, 0, 0, 0, 1, 0, 2, 0, 26, 1, 1, 16, 252, 248, 142, 63, 0, 0, 13, 0, 0, 0, 13, 0, 0, 0}

	err := rows.Decode(data)
	require.NoError(t, err)
	require.Equal(t, int32(16270), rows.Rows[0][0])
}

func TestParseJson(t *testing.T) {
	// Table format:
	// mysql> desc t10;
	// +-------+---------------+------+-----+---------+-------+
	// | Field | Type          | Null | Key | Default | Extra |
	// +-------+---------------+------+-----+---------+-------+
	// | c1    | json          | YES  |     | NULL    |       |
	// | c2    | decimal(10,0) | YES  |     | NULL    |       |
	// +-------+---------------+------+-----+---------+-------+

	// CREATE TABLE `t10` (
	//   `c1` json DEFAULT NULL,
	//   `c2` decimal(10,0)
	// ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	//nolint:misspell
	// INSERT INTO `t10` (`c2`) VALUES (1);
	// INSERT INTO `t10` (`c1`, `c2`) VALUES ('{"key1": "value1", "key2": "value2"}', 1);
	// test json deserialization
	// INSERT INTO `t10`(`c1`,`c2`) VALUES ('{"text":"Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus. Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet adipiscing sem neque sed ipsum. Nam quam nunc, blandit vel, luctus pulvinar, hendrerit id, lorem. Maecenas nec odio et ante tincidunt tempus. Donec vitae sapien ut libero venenatis faucibus. Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum sodales, augue velit cursus nunc, quis gravida magna mi a libero. Fusce vulputate eleifend sapien. Vestibulum purus quam, scelerisque ut, mollis sed, nonummy id, metus. Nullam accumsan lorem in dui. Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu. Duis arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis, ipsum. Sed aliquam ultrices mauris. Integer ante arcu, accumsan a, consectetuer eget, posuere ut, mauris. Praesent adipiscing. Phasellus ullamcorper ipsum rutrum nunc. Nunc nonummy metus. Vestibulum volutpat pretium libero. Cras id dui. Aenean ut eros et nisl sagittis vestibulum. Nullam nulla eros, ultricies sit amet, nonummy id, imperdiet feugiat, pede. Sed lectus. Donec mollis hendrerit risus. Phasellus nec sem in justo pellentesque facilisis. Etiam imperdiet imperdiet orci. Nunc nec neque. Phasellus leo dolor, tempus non, auctor et, hendrerit quis, nisi. Curabitur ligula sapien, tincidunt non, euismod vitae, posuere imperdiet, leo. Maecenas malesuada. Praesent congue erat at massa. Sed cursus turpis vitae tortor. Donec posuere vulputate arcu. Phasellus accumsan cursus velit. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Sed aliquam, nisi quis porttitor congue, elit erat euismod orci, ac"}',101);
	tableMapEventData := []byte("m\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x03t10\x00\x02\xf5\xf6\x03\x04\n\x00\x03")

	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	require.NoError(t, err)

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	tbls := [][]byte{
		[]byte("m\x00\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfd\x80\x00\x00\x00\x01"),
		[]byte("m\x00\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfc)\x00\x00\x00\x00\x02\x00(\x00\x12\x00\x04\x00\x16\x00\x04\x00\f\x1a\x00\f!\x00key1key2\x06value1\x06value2\x80\x00\x00\x00\x01"),
	}

	for _, tbl := range tbls {
		rows.Rows = nil
		err = rows.Decode(tbl)
		require.NoError(t, err)
		require.Equal(t, "1", rows.Rows[0][1])
	}

	//nolint:misspell
	longTbls := [][]byte{
		[]byte("m\x00\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfc\xd0\n\x00\x00\x00\x01\x00\xcf\n\v\x00\x04\x00\f\x0f\x00text\xbe\x15Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus. Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet adipiscing sem neque sed ipsum. Nam quam nunc, blandit vel, luctus pulvinar, hendrerit id, lorem. Maecenas nec odio et ante tincidunt tempus. Donec vitae sapien ut libero venenatis faucibus. Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum sodales, augue velit cursus nunc, quis gravida magna mi a libero. Fusce vulputate eleifend sapien. Vestibulum purus quam, scelerisque ut, mollis sed, nonummy id, metus. Nullam accumsan lorem in dui. Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu. Duis arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis, ipsum. Sed aliquam ultrices mauris. Integer ante arcu, accumsan a, consectetuer eget, posuere ut, mauris. Praesent adipiscing. Phasellus ullamcorper ipsum rutrum nunc. Nunc nonummy metus. Vestibulum volutpat pretium libero. Cras id dui. Aenean ut eros et nisl sagittis vestibulum. Nullam nulla eros, ultricies sit amet, nonummy id, imperdiet feugiat, pede. Sed lectus. Donec mollis hendrerit risus. Phasellus nec sem in justo pellentesque facilisis. Etiam imperdiet imperdiet orci. Nunc nec neque. Phasellus leo dolor, tempus non, auctor et, hendrerit quis, nisi. Curabitur ligula sapien, tincidunt non, euismod vitae, posuere imperdiet, leo. Maecenas malesuada. Praesent congue erat at massa. Sed cursus turpis vitae tortor. Donec posuere vulputate arcu. Phasellus accumsan cursus velit. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Sed aliquam, nisi quis porttitor congue, elit erat euismod orci, ac\x80\x00\x00\x00e"),
	}

	for _, ltbl := range longTbls {
		rows.Rows = nil
		err = rows.Decode(ltbl)
		require.NoError(t, err)
		require.Equal(t, "101", rows.Rows[0][1])
	}
}

func TestParseJsonDecimal(t *testing.T) {
	// Table format:
	// mysql> desc t10;
	// +-------+---------------+------+-----+---------+-------+
	// | Field | Type          | Null | Key | Default | Extra |
	// +-------+---------------+------+-----+---------+-------+
	// | c1    | json          | YES  |     | NULL    |       |
	// | c2    | decimal(10,0) | YES  |     | NULL    |       |
	// +-------+---------------+------+-----+---------+-------+

	// CREATE TABLE `t10` (
	//   `c1` json DEFAULT NULL,
	//   `c2` decimal(10,0)
	// ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

	//nolint:misspell
	// INSERT INTO `t10` (`c2`) VALUES (1);
	// INSERT INTO `t10` (`c1`, `c2`) VALUES ('{"key1": "value1", "key2": "value2"}', 1);
	// test json deserialization
	// INSERT INTO `t10`(`c1`,`c2`) VALUES ('{"text":"Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus. Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet adipiscing sem neque sed ipsum. Nam quam nunc, blandit vel, luctus pulvinar, hendrerit id, lorem. Maecenas nec odio et ante tincidunt tempus. Donec vitae sapien ut libero venenatis faucibus. Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum sodales, augue velit cursus nunc, quis gravida magna mi a libero. Fusce vulputate eleifend sapien. Vestibulum purus quam, scelerisque ut, mollis sed, nonummy id, metus. Nullam accumsan lorem in dui. Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu. Duis arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis, ipsum. Sed aliquam ultrices mauris. Integer ante arcu, accumsan a, consectetuer eget, posuere ut, mauris. Praesent adipiscing. Phasellus ullamcorper ipsum rutrum nunc. Nunc nonummy metus. Vestibulum volutpat pretium libero. Cras id dui. Aenean ut eros et nisl sagittis vestibulum. Nullam nulla eros, ultricies sit amet, nonummy id, imperdiet feugiat, pede. Sed lectus. Donec mollis hendrerit risus. Phasellus nec sem in justo pellentesque facilisis. Etiam imperdiet imperdiet orci. Nunc nec neque. Phasellus leo dolor, tempus non, auctor et, hendrerit quis, nisi. Curabitur ligula sapien, tincidunt non, euismod vitae, posuere imperdiet, leo. Maecenas malesuada. Praesent congue erat at massa. Sed cursus turpis vitae tortor. Donec posuere vulputate arcu. Phasellus accumsan cursus velit. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Sed aliquam, nisi quis porttitor congue, elit erat euismod orci, ac"}',101);
	tableMapEventData := []byte("m\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x03t10\x00\x02\xf5\xf6\x03\x04\n\x00\x03")

	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	require.NoError(t, err)

	rows := RowsEvent{useDecimal: true}
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	tbls := [][]byte{
		[]byte("m\x00\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfd\x80\x00\x00\x00\x01"),
		[]byte("m\x00\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfc)\x00\x00\x00\x00\x02\x00(\x00\x12\x00\x04\x00\x16\x00\x04\x00\f\x1a\x00\f!\x00key1key2\x06value1\x06value2\x80\x00\x00\x00\x01"),
	}

	for _, tbl := range tbls {
		rows.Rows = nil
		err = rows.Decode(tbl)
		require.NoError(t, err)
		require.True(t, rows.Rows[0][1].(decimal.Decimal).Equal(decimal.NewFromFloat(1)))
	}

	//nolint:misspell
	longTbls := [][]byte{
		[]byte("m\x00\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfc\xd0\n\x00\x00\x00\x01\x00\xcf\n\v\x00\x04\x00\f\x0f\x00text\xbe\x15Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus. Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet adipiscing sem neque sed ipsum. Nam quam nunc, blandit vel, luctus pulvinar, hendrerit id, lorem. Maecenas nec odio et ante tincidunt tempus. Donec vitae sapien ut libero venenatis faucibus. Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum sodales, augue velit cursus nunc, quis gravida magna mi a libero. Fusce vulputate eleifend sapien. Vestibulum purus quam, scelerisque ut, mollis sed, nonummy id, metus. Nullam accumsan lorem in dui. Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu. Duis arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis, ipsum. Sed aliquam ultrices mauris. Integer ante arcu, accumsan a, consectetuer eget, posuere ut, mauris. Praesent adipiscing. Phasellus ullamcorper ipsum rutrum nunc. Nunc nonummy metus. Vestibulum volutpat pretium libero. Cras id dui. Aenean ut eros et nisl sagittis vestibulum. Nullam nulla eros, ultricies sit amet, nonummy id, imperdiet feugiat, pede. Sed lectus. Donec mollis hendrerit risus. Phasellus nec sem in justo pellentesque facilisis. Etiam imperdiet imperdiet orci. Nunc nec neque. Phasellus leo dolor, tempus non, auctor et, hendrerit quis, nisi. Curabitur ligula sapien, tincidunt non, euismod vitae, posuere imperdiet, leo. Maecenas malesuada. Praesent congue erat at massa. Sed cursus turpis vitae tortor. Donec posuere vulputate arcu. Phasellus accumsan cursus velit. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Sed aliquam, nisi quis porttitor congue, elit erat euismod orci, ac\x80\x00\x00\x00e"),
	}

	for _, ltbl := range longTbls {
		rows.Rows = nil
		err = rows.Decode(ltbl)
		require.NoError(t, err)
		require.True(t, rows.Rows[0][1].(decimal.Decimal).Equal(decimal.NewFromFloat(101)))
	}
}

func TestEnum(t *testing.T) {
	// mysql> desc aenum;
	// +-------+-------------------------------------------+------+-----+---------+-------+
	// | Field | Type                                      | Null | Key | Default | Extra |
	// +-------+-------------------------------------------+------+-----+---------+-------+
	// | id    | int(11)                                   | YES  |     | NULL    |       |
	// | aset  | enum('0','1','2','3','4','5','6','7','8') | YES  |     | NULL    |       |
	// +-------+-------------------------------------------+------+-----+---------+-------+
	// 2 rows in set (0.00 sec)
	//
	// insert into aenum(id, aset) values(1, '0');
	tableMapEventData := []byte("\x42\x0f\x00\x00\x00\x00\x01\x00\x05\x74\x74\x65\x73\x74\x00\x05")
	tableMapEventData = append(tableMapEventData, []byte("\x61\x65\x6e\x75\x6d\x00\x02\x03\xfe\x02\xf7\x01\x03")...)
	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	require.NoError(t, err)

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	data := []byte("\x42\x0f\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfc\x01\x00\x00\x00\x01")

	rows.Rows = nil
	err = rows.Decode(data)
	require.NoError(t, err)
	require.Equal(t, int64(1), rows.Rows[0][1])
}

func TestMultiBytesEnum(t *testing.T) {
	// CREATE TABLE numbers (
	// 	id int auto_increment,
	// 	num ENUM( '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116', '117', '118', '119', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129', '130', '131', '132', '133', '134', '135', '136', '137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156', '157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176', '177', '178', '179', '180', '181', '182', '183', '184', '185', '186', '187', '188', '189', '190', '191', '192', '193', '194', '195', '196', '197', '198', '199', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236', '237', '238', '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '253', '254', '255','256','257'

	// ),
	// primary key(id)
	// );

	//
	// insert into numbers(num) values ('0'), ('256');
	tableMapEventData := []byte("\x84\x0f\x00\x00\x00\x00\x01\x00\x05\x74\x74\x65\x73\x74\x00\x07")
	tableMapEventData = append(tableMapEventData, []byte("\x6e\x75\x6d\x62\x65\x72\x73\x00\x02\x03\xfe\x02\xf7\x02\x02")...)
	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	require.NoError(t, err)

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	data := []byte("\x84\x0f\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfc\x01\x00\x00\x00\x01\x00\xfc\x02\x00\x00\x00\x01\x01")

	rows.Rows = nil
	err = rows.Decode(data)
	require.NoError(t, err)
	require.Equal(t, int64(1), rows.Rows[0][1])
	require.Equal(t, int64(257), rows.Rows[1][1])
}

func TestSet(t *testing.T) {
	// mysql> desc aset;
	// +--------+---------------------------------------------------------------------------------------+------+-----+---------+-------+
	// | Field  | Type                                                                                  | Null | Key | Default | Extra |
	// +--------+---------------------------------------------------------------------------------------+------+-----+---------+-------+
	// | id     | int(11)                                                                               | YES  |     | NULL    |       |
	// | region | set('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18') | YES  |     | NULL    |       |
	// +--------+---------------------------------------------------------------------------------------+------+-----+---------+-------+
	// 2 rows in set (0.00 sec)
	//
	// insert into aset(id, region) values(1, '1,3');

	tableMapEventData := []byte("\xe7\x0e\x00\x00\x00\x00\x01\x00\x05\x74\x74\x65\x73\x74\x00\x04")
	tableMapEventData = append(tableMapEventData, []byte("\x61\x73\x65\x74\x00\x02\x03\xfe\x02\xf8\x03\x03")...)
	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	require.NoError(t, err)

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	data := []byte("\xe7\x0e\x00\x00\x00\x00\x01\x00\x02\x00\x02\xff\xfc\x01\x00\x00\x00\x05\x00\x00")

	rows.Rows = nil
	err = rows.Decode(data)
	require.NoError(t, err)
	require.Equal(t, int64(5), rows.Rows[0][1])
}

func TestJsonNull(t *testing.T) {
	// Table:
	// desc hj_order_preview
	// +------------------+------------+------+-----+-------------------+----------------+
	// | Field            | Type       | Null | Key | Default           | Extra          |
	// +------------------+------------+------+-----+-------------------+----------------+
	// | id               | int(13)    | NO   | PRI | <null>            | auto_increment |
	// | buyer_id         | bigint(13) | NO   |     | <null>            |                |
	// | order_sn         | bigint(13) | NO   |     | <null>            |                |
	// | order_detail     | json       | NO   |     | <null>            |                |
	// | is_del           | tinyint(1) | NO   |     | 0                 |                |
	// | add_time         | int(13)    | NO   |     | <null>            |                |
	// | last_update_time | timestamp  | NO   |     | CURRENT_TIMESTAMP |                |
	// +------------------+------------+------+-----+-------------------+----------------+
	// insert into hj_order_preview
	// (id, buyer_id, order_sn, is_del, add_time, last_update_time)
	// values (1, 95891865464386, 13376222192996417, 0, 1479983995, 1479983995)

	tableMapEventData := []byte("r\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x10hj_order_preview\x00\a\x03\b\b\xf5\x01\x03\x11\x02\x04\x00\x00")

	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	require.NoError(t, err)

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	data := []byte("r\x00\x00\x00\x00\x00\x01\x00\x02\x00\a\xff\x80\x01\x00\x00\x00B\ue4d06W\x00\x00A\x10@l\x9a\x85/\x00\x00\x00\x00\x00\x00{\xc36X\x00\x00\x00\x00")

	rows.Rows = nil
	err = rows.Decode(data)
	require.NoError(t, err)
	require.Len(t, rows.Rows[0][3], 0)
}

func TestJsonCompatibility(t *testing.T) {
	// Table:
	// mysql> desc t11;
	// +----------+--------------+------+-----+---------+-------------------+
	// | Field    | Type         | Null | Key | Default | Extra             |
	// +----------+--------------+------+-----+---------+-------------------+
	// | id       | int(11)      | YES  |     | NULL    |                   |
	// | cfg      | varchar(100) | YES  |     | NULL    |                   |
	// | cfg_json | json         | YES  |     | NULL    | VIRTUAL GENERATED |
	// | age      | int(11)      | YES  |     | NULL    |                   |
	// +----------+--------------+------+-----+---------+-------------------+
	// mysql> insert into t11(id, cfg) values (1, '{}');

	// test json deserialization
	// mysql> update t11 set cfg = '{"a":1234}' where id = 1;
	// mysql> update test set cfg = '{}' where id = 1;

	tableMapEventData := []byte("l\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x03t11\x00\x04\x03\x0f\xf5\x03\x03d\x00\x04\x0f")

	tableMapEvent := new(TableMapEvent)
	tableMapEvent.tableIDSize = 6
	err := tableMapEvent.Decode(tableMapEventData)
	require.NoError(t, err)

	rows := new(RowsEvent)
	rows.tableIDSize = 6
	rows.tables = make(map[uint64]*TableMapEvent)
	rows.tables[tableMapEvent.TableID] = tableMapEvent
	rows.Version = 2

	data := []byte("l\x00\x00\x00\x00\x00\x01\x00\x02\x00\x04\xff\xf8\x01\x00\x00\x00\x02{}\x05\x00\x00\x00\x00\x00\x00\x04\x00")
	rows.Rows = nil
	err = rows.Decode(data)
	require.NoError(t, err)
	require.Equal(t, "{}", rows.Rows[0][2])

	// after MySQL 5.7.22
	data = []byte("l\x00\x00\x00\x00\x00\x01\x00\x02\x00\x04\xff\xff\xf8\x01\x00\x00\x00\x02{}\x05\x00\x00\x00\x00\x00\x00\x04\x00\xf8\x01\x00\x00\x00\n{\"a\":1234}\r\x00\x00\x00\x00\x01\x00\x0c\x00\x0b\x00\x01\x00\x05\xd2\x04a")
	rows.Rows = nil
	err = rows.Decode(data)
	require.NoError(t, err)
	require.Equal(t, "{}", rows.Rows[1][2])
	require.Equal(t, "{\"a\":1234}", rows.Rows[2][2])

	data = []byte("l\x00\x00\x00\x00\x00\x01\x00\x02\x00\x04\xff\xff\xf8\x01\x00\x00\x00\n{\"a\":1234}\r\x00\x00\x00\x00\x01\x00\x0c\x00\x0b\x00\x01\x00\x05\xd2\x04a\xf8\x01\x00\x00\x00\x02{}\x05\x00\x00\x00\x00\x00\x00\x04\x00")
	rows.Rows = nil
	err = rows.Decode(data)
	require.NoError(t, err)
	require.Equal(t, "{\"a\":1234}", rows.Rows[1][2])
	require.Equal(t, "{}", rows.Rows[2][2])

	// before MySQL 5.7.22
	rows.ignoreJSONDecodeErr = true
	data = []byte("l\x00\x00\x00\x00\x00\x01\x00\x02\x00\x04\xff\xff\xf8\x01\x00\x00\x00\x02{}\x05\x00\x00\x00\x00\x01\x00\x0c\x00\xf8\x01\x00\x00\x00\n{\"a\":1234}\r\x00\x00\x00\x00\x01\x00\x0c\x00\x0b\x00\x01\x00\x05\xd2\x04a")
	rows.Rows = nil
	err = rows.Decode(data)
	require.NoError(t, err)
	require.Equal(t, "null", rows.Rows[1][2])
	require.Equal(t, "{\"a\":1234}", rows.Rows[2][2])

	rows.ignoreJSONDecodeErr = false
	data = []byte("l\x00\x00\x00\x00\x00\x01\x00\x02\x00\x04\xff\xff\xf8\x01\x00\x00\x00\n{\"a\":1234}\r\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x01\x00\x05\xd2\x04a\xf8\x01\x00\x00\x00\x02{}\x05\x00\x00\x00\x00\x00\x00\x04\x00")
	rows.Rows = nil
	err = rows.Decode(data)
	require.NoError(t, err)
	// this value is wrong in binlog, but can be parsed without error
	require.Equal(t, "{}", rows.Rows[1][2])
	require.Equal(t, "{}", rows.Rows[2][2])
}

func TestDecodeDatetime2(t *testing.T) {
	testcases := []struct {
		data        []byte
		dec         uint16
		getFracTime bool
		expected    string
	}{
		{[]byte("\xfe\xf3\xff\x7e\xfb"), 0, true, "9999-12-31 23:59:59"},
		{[]byte("\x99\x9a\xb8\xf7\xaa"), 0, true, "2016-10-28 15:30:42"},
		{[]byte("\x99\x98\x38\xf7\xaa"), 0, false, "2016-00-28 15:30:42"},
		{[]byte("\x99\x9a\x80\xf7\xaa"), 0, false, "2016-10-00 15:30:42"},
		{[]byte("\x99\x02\xc2\x00\x00"), 0, true, "1970-01-01 00:00:00"},
		{[]byte("\x80\x00\x00\x00\x00"), 0, false, "0000-00-00 00:00:00"},
		{[]byte("\x80\x00\x02\xf1\x05"), 0, false, "0000-00-01 15:04:05"},
		{[]byte("\x80\x03\x82\x00\x00"), 0, false, "0001-01-01 00:00:00"},
		{[]byte("\x80\x03\x82\x00\x00\x0c"), uint16(2), false, "0001-01-01 00:00:00.12"},
		{[]byte("\x80\x03\x82\x00\x00\x04\xd3"), uint16(4), false, "0001-01-01 00:00:00.1235"},
		{[]byte("\x80\x03\x82\x00\x00\x01\xe2\x40"), uint16(6), false, "0001-01-01 00:00:00.123456"},
	}
	for _, tc := range testcases {
		for _, parseTime := range []bool{true, false} {
			value, _, err := decodeDatetime2(tc.data, tc.dec, parseTime)
			require.NoError(t, err)
			switch v := value.(type) {
			case fracTime:
				require.True(t, parseTime)
				require.True(t, tc.getFracTime)
				require.Equal(t, tc.expected, v.String())
			case string:
				require.False(t, parseTime && tc.getFracTime)
				require.Equal(t, tc.expected, v)
			default:
				require.FailNow(t, "invalid value type: %T", value)
			}
		}
	}
}

func TestTableMapNullable(t *testing.T) {
	/*
		create table _null (c1 int null, c2 int not null default '2', c3 timestamp default now(), c4 text);
	*/
	nullables := []bool{true, false, false, true}
	testcases := [][]byte{
		// mysql 5.7
		[]byte("z\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x05_null\x00\x04\x03\x03\x11\xfc\x02\x00\x02\t"),
		// mysql 8.0
		[]byte("z\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x05_null\x00\x04\x03\x03\x11\xfc\x02\x00\x02\t\x01\x01\x00\x02\x01\xe0\x04\f\x02c1\x02c2\x02c3\x02c4"),
		// mariadb 10.4
		[]byte("\x1e\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x05_null\x00\x04\x03\x03\x11\xfc\x02\x00\x02\t"),
		// mariadb 10.5
		[]byte("\x1d\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x05_null\x00\x04\x03\x03\x11\xfc\x02\x00\x02\t\x01\x01\x00\x02\x01\xe0\x04\f\x02c1\x02c2\x02c3\x02c4"),
	}

	for _, tc := range testcases {
		tableMapEvent := new(TableMapEvent)
		tableMapEvent.tableIDSize = 6
		err := tableMapEvent.Decode(tc)
		require.NoError(t, err)
		require.Equal(t, int(tableMapEvent.ColumnCount), len(nullables))
		for i := 0; i < int(tableMapEvent.ColumnCount); i++ {
			available, nullable := tableMapEvent.Nullable(i)
			require.True(t, available)
			require.Equal(t, nullables[i], nullable)
		}
	}
}

func TestTableMapOptMetaNames(t *testing.T) {
	/*
		CREATE TABLE `_types` (
			`b_bit` bit(64) NOT NULL DEFAULT b'0',

			`n_boolean` boolean not null default '0',
			`n_tinyint` tinyint not null default '0',
			`n_smallint` smallint not null default '0',
			`n_mediumint` mediumint not null default '0',
			`n_int` int not null default '0',
			`n_bigint` bigint not null default '0',
			`n_decimal` decimal(65,30) not null default '0',
			`n_float` float not null default '0',
			`n_double` double not null default '0',

			`nu_tinyint` tinyint unsigned not null default '0',
			`nu_smallint` smallint unsigned not null default '0',
			`nu_mediumint` mediumint unsigned not null default '0',
			`nu_int` int unsigned not null default '0',
			`nu_bigint` bigint unsigned not null default '0',
			`nu_decimal` decimal(65,30) unsigned not null default '0',
			`nu_float` float unsigned not null default '0',
			`nu_double` double unsigned not null default '0',

			`t_year` year default null,
			`t_date` date default null,
			`t_time` time default null,
			`t_ftime` time(6) default null,
			`t_datetime` datetime default null,
			`t_fdatetime` datetime(6) default null,
			`t_timestamp` timestamp default current_timestamp,
			`t_ftimestamp` timestamp(6) default current_timestamp(6),

			`c_char` char(255) not null default '',
			`c_varchar` varchar(255) not null default '',
			`c_binary` binary(64) not null default '',
			`c_varbinary` varbinary(64) not null default '',
			`c_tinyblob` tinyblob,
			`c_blob` blob,
			`c_mediumblob` mediumblob,
			`c_longblob` longblob,
			`c_tinytext` tinytext,
			`c_text` text,
			`c_mediumtext` mediumtext,
			`c_longtext` longtext,

			`e_enum` enum('a','b') default 'a',
			`s_set` set('1','2') default '1',
			`g_geometry` geometry DEFAULT NULL,
			`j_json` json DEFAULT NULL
		);
		insert into _types values ();
	*/
	colNames := []string{
		"b_bit",
		"n_boolean",
		"n_tinyint",
		"n_smallint",
		"n_mediumint",
		"n_int",
		"n_bigint",
		"n_decimal",
		"n_float",
		"n_double",
		"nu_tinyint",
		"nu_smallint",
		"nu_mediumint",
		"nu_int",
		"nu_bigint",
		"nu_decimal",
		"nu_float",
		"nu_double",
		"t_year",
		"t_date",
		"t_time",
		"t_ftime",
		"t_datetime",
		"t_fdatetime",
		"t_timestamp",
		"t_ftimestamp",
		"c_char",
		"c_varchar",
		"c_binary",
		"c_varbinary",
		"c_tinyblob",
		"c_blob",
		"c_mediumblob",
		"c_longblob",
		"c_tinytext",
		"c_text",
		"c_mediumtext",
		"c_longtext",
		"e_enum",
		"s_set",
		"g_geometry",
		"j_json",
	}
	enumVals := [][]string{{"a", "b"}}
	setVals := [][]string{{"1", "2"}}

	testcases := []struct {
		data     []byte
		hasNames bool
	}{
		// mysql 5.7
		{data: []byte("u\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x06_types\x00*\x10\x01\x01\x02\t\x03\b\xf6\x04\x05\x01\x02\t\x03\b\xf6\x04\x05\r\n\x13\x13\x12\x12\x11\x11\xfe\x0f\xfe\x0f\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfe\xfe\xff\xf5&\x00\bA\x1e\x04\bA\x1e\x04\b\x00\x06\x00\x06\x00\x06\xce\xfc\xfc\x03\xfe@@\x00\x01\x02\x03\x04\x01\x02\x03\x04\xf7\x01\xf8\x01\x04\x04\x00\x00\xfc\xc0\xff\x03")},
		// mysql 8.0
		{data: []byte("j\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x06_types\x00*\x10\x01\x01\x02\t\x03\b\xf6\x04\x05\x01\x02\t\x03\b\xf6\x04\x05\r\n\x13\x13\x12\x12\x11\x11\xfe\x0f\xfe\x0f\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfe\xfe\xff\xf5&\x00\bA\x1e\x04\bA\x1e\x04\b\x00\x06\x00\x06\x00\x06\xce\xfc\xfc\x03\xfe@@\x00\x01\x02\x03\x04\x01\x02\x03\x04\xf7\x01\xf8\x01\x04\x04\x00\x00\xfc\xc3\xff\x03\x01\x03\x00\u007f\x80\x03\f\xe0\xe0??????\xe0\xe0\xe0\xe0\a\x01\x00\x04\xfc\x94\x01\x05b_bit\tn_boolean\tn_tinyint\nn_smallint\vn_mediumint\x05n_int\bn_bigint\tn_decimal\an_float\bn_double\nnu_tinyint\vnu_smallint\fnu_mediumint\x06nu_int\tnu_bigint\nnu_decimal\bnu_float\tnu_double\x06t_year\x06t_date\x06t_time\at_ftime\nt_datetime\vt_fdatetime\vt_timestamp\ft_ftimestamp\x06c_char\tc_varchar\bc_binary\vc_varbinary\nc_tinyblob\x06c_blob\fc_mediumblob\nc_longblob\nc_tinytext\x06c_text\fc_mediumtext\nc_longtext\x06e_enum\x05s_set\ng_geometry\x06j_json\n\x01\xe0\x05\x05\x02\x011\x012\x06\x05\x02\x01a\x01b"), hasNames: true},
		// mariadb 10.4
		{data: []byte("\x1b\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x06_types\x00*\x10\x01\x01\x02\t\x03\b\xf6\x04\x05\x01\x02\t\x03\b\xf6\x04\x05\r\n\x13\x13\x12\x12\x11\x11\xfe\x0f\xfe\x0f\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfe\xfe\xff\xfc&\x00\bA\x1e\x04\bA\x1e\x04\b\x00\x06\x00\x06\x00\x06\xce\xfc\xfc\x03\xfe@@\x00\x01\x02\x03\x04\x01\x02\x03\x04\xf7\x01\xf8\x01\x04\x04\x00\x00\xfc\xc0\xff\x03")},
		// mariadb 10.5
		{data: []byte("\x1a\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x06_types\x00*\x10\x01\x01\x02\t\x03\b\xf6\x04\x05\x01\x02\t\x03\b\xf6\x04\x05\r\n\x13\x13\x12\x12\x11\x11\xfe\x0f\xfe\x0f\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfe\xfe\xff\xfc&\x00\bA\x1e\x04\bA\x1e\x04\b\x00\x06\x00\x06\x00\x06\xce\xfc\xfc\x03\xfe@@\x00\x01\x02\x03\x04\x01\x02\x03\x04\xf7\x01\xf8\x01\x04\x04\x00\x00\xfc\xc0\xff\x03\x01\x03\x00\u007f\xc0\x03\x0e\xe0\xe0??????\xe0\xe0\xe0\xe0?.\a\x01\x00\x04\xfc\x94\x01\x05b_bit\tn_boolean\tn_tinyint\nn_smallint\vn_mediumint\x05n_int\bn_bigint\tn_decimal\an_float\bn_double\nnu_tinyint\vnu_smallint\fnu_mediumint\x06nu_int\tnu_bigint\nnu_decimal\bnu_float\tnu_double\x06t_year\x06t_date\x06t_time\at_ftime\nt_datetime\vt_fdatetime\vt_timestamp\ft_ftimestamp\x06c_char\tc_varchar\bc_binary\vc_varbinary\nc_tinyblob\x06c_blob\fc_mediumblob\nc_longblob\nc_tinytext\x06c_text\fc_mediumtext\nc_longtext\x06e_enum\x05s_set\ng_geometry\x06j_json\n\x01\xe0\x05\x05\x02\x011\x012\x06\x05\x02\x01a\x01b"), hasNames: true},
	}

	for _, tc := range testcases {
		tableMapEvent := new(TableMapEvent)
		tableMapEvent.tableIDSize = 6
		err := tableMapEvent.Decode(tc.data)
		require.NoError(t, err)

		if tc.hasNames {
			require.Equal(t, colNames, tableMapEvent.ColumnNameString())
			require.Equal(t, setVals, tableMapEvent.SetStrValueString())
			require.Equal(t, enumVals, tableMapEvent.EnumStrValueString())
		} else {
			require.Equal(t, []string(nil), tableMapEvent.ColumnNameString())
			require.Equal(t, [][]string(nil), tableMapEvent.SetStrValueString())
			require.Equal(t, [][]string(nil), tableMapEvent.EnumStrValueString())
		}
	}
}

func TestTableMapOptMetaPrimaryKey(t *testing.T) {
	/*
		create table _prim (id2 int, col varchar(30), id1 bigint, primary key (id1, id2));
	*/
	case1PrimaryKey := []uint64{2, 0}
	case1PrimaryKeyPrefix := []uint64{0, 0}

	/*
		create table _prim2 (col1 int, id1 char(10), col2 int, id2 varchar(20), primary key (id1, id2(10)));
	*/
	case2PrimaryKey := []uint64{1, 3}
	case2PrimaryKeyPrefix := []uint64{0, 10}

	testcases := []struct {
		data                     []byte
		expectedPrimaryKey       []uint64
		expectedPrimaryKeyPrefix []uint64
	}{
		{
			// mysql 5.7, case1
			data:                     []byte("w\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x05_prim\x00\x03\x03\x0f\b\x02x\x00\x02"),
			expectedPrimaryKey:       []uint64(nil),
			expectedPrimaryKeyPrefix: []uint64(nil),
		},
		{
			// mysql 8.0, case1
			data:                     []byte("l\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x05_prim\x00\x03\x03\x0f\b\x02x\x00\x02\x01\x01\x00\x02\x01\xe0\x04\f\x03id2\x03col\x03id1\b\x02\x02\x00"),
			expectedPrimaryKey:       case1PrimaryKey,
			expectedPrimaryKeyPrefix: case1PrimaryKeyPrefix,
		},
		{
			// mariadb 10.4, case1
			data:                     []byte("\x1c\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x05_prim\x00\x03\x03\x0f\b\x02x\x00\x02"),
			expectedPrimaryKey:       []uint64(nil),
			expectedPrimaryKeyPrefix: []uint64(nil),
		},
		{
			// mariadb 10.5, case1
			data:                     []byte("\x1b\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x05_prim\x00\x03\x03\x0f\b\x02x\x00\x02\x01\x01\x00\x02\x01\xe0\x04\f\x03id2\x03col\x03id1\b\x02\x02\x00"),
			expectedPrimaryKey:       case1PrimaryKey,
			expectedPrimaryKeyPrefix: case1PrimaryKeyPrefix,
		},
		{
			// mysql 5.7, case2
			data:                     []byte("y\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x06_prim2\x00\x04\x03\xfe\x03\x0f\x04\xfe(P\x00\x05"),
			expectedPrimaryKey:       []uint64(nil),
			expectedPrimaryKeyPrefix: []uint64(nil),
		},
		{
			// mysql 8.0, case2
			data:                     []byte("m\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x06_prim2\x00\x04\x03\xfe\x03\x0f\x04\xfe(P\x00\x05\x01\x01\x00\x02\x01\xe0\x04\x12\x04col1\x03id1\x04col2\x03id2\t\x04\x01\x00\x03\n"),
			expectedPrimaryKey:       case2PrimaryKey,
			expectedPrimaryKeyPrefix: case2PrimaryKeyPrefix,
		},
		{
			// mariadb 10.4, case2
			data:                     []byte("\x1d\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x06_prim2\x00\x04\x03\xfe\x03\x0f\x04\xfe(P\x00\x05"),
			expectedPrimaryKey:       []uint64(nil),
			expectedPrimaryKeyPrefix: []uint64(nil),
		},
		{
			// mariadb 10.5, case2
			data:                     []byte("\x1c\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x06_prim2\x00\x04\x03\xfe\x03\x0f\x04\xfe(P\x00\x05\x01\x01\x00\x02\x01\xe0\x04\x12\x04col1\x03id1\x04col2\x03id2\t\x04\x01\x00\x03\n"),
			expectedPrimaryKey:       case2PrimaryKey,
			expectedPrimaryKeyPrefix: case2PrimaryKeyPrefix,
		},
	}

	for _, tc := range testcases {
		tableMapEvent := new(TableMapEvent)
		tableMapEvent.tableIDSize = 6
		err := tableMapEvent.Decode(tc.data)
		require.NoError(t, err)
		require.Equal(t, tc.expectedPrimaryKey, tableMapEvent.PrimaryKey)
		require.Equal(t, tc.expectedPrimaryKeyPrefix, tableMapEvent.PrimaryKeyPrefix)
	}
}

func TestTableMapOptMetaVisibility(t *testing.T) {
	/*
		SET GLOBAL binlog_row_image = FULL;
		SET GLOBAL binlog_row_metadata = FULL;	-- if applicable

		CREATE DATABASE test;
		USE test;
	*/

	/*
		CREATE TABLE _visibility(
		    `col0` INT INVISIBLE,
		    `col1` INT,
		    `col2` INT INVISIBLE,
		    `col3` INT,
		    `col4` INT,
		    `col5` INT INVISIBLE,
		    `col6` INT INVISIBLE,
		    `col7` INT INVISIBLE,
		    `col8` INT,
		    `col9` INT INVISIBLE,
		    `col10` INT INVISIBLE
		);
	*/
	case1VisibilityBitmap := []byte{0x58, 0x80}
	case1VisibilityMap := map[int]bool{
		0:  false,
		1:  true,
		2:  false,
		3:  true,
		4:  true,
		5:  false,
		6:  false,
		7:  false,
		8:  true,
		9:  false,
		10: false,
	}

	/*
		CREATE TABLE _visibility(
		    `col0` INT,
		    `col1` INT,
		    `col2` INT,
		    `col3` INT,
		    `col4` INT,
		    `col5` INT,
		    `col6` INT,
		    `col7` INT,
		    `col8` INT,
		    `col9` INT,
		    `col10` INT
		);
	*/
	case2VisibilityBitmap := []byte{0xff, 0xe0}
	case2VisibilityMap := map[int]bool{
		0:  true,
		1:  true,
		2:  true,
		3:  true,
		4:  true,
		5:  true,
		6:  true,
		7:  true,
		8:  true,
		9:  true,
		10: true,
	}

	// Invisible column and INVISIBLE keyword is available only on MySQL 8.0.23+
	testcases := []struct {
		data                     []byte
		expectedVisibilityBitmap []byte
		expectedVisibilityMap    map[int]bool
	}{
		{
			// mysql 8.0, case1
			data:                     []byte("^\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x0b_visibility\x00\x0b\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x00\xff\x07\x01\x02\x00\x00\x048\x04col0\x04col1\x04col2\x04col3\x04col4\x04col5\x04col6\x04col7\x04col8\x04col9\x05col10\x0c\x02X\x80"),
			expectedVisibilityBitmap: case1VisibilityBitmap,
			expectedVisibilityMap:    case1VisibilityMap,
		},
		{
			// mysql 5.7, case2
			data:                     []byte("m\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x0b_visibility\x00\x0b\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x00\xff\x07"),
			expectedVisibilityBitmap: []byte(nil),
			expectedVisibilityMap:    map[int]bool(nil),
		},
		{
			// mysql 8.0, case2
			data:                     []byte("^\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x0b_visibility\x00\x0b\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x00\xff\x07\x01\x02\x00\x00\x048\x04col0\x04col1\x04col2\x04col3\x04col4\x04col5\x04col6\x04col7\x04col8\x04col9\x05col10\x0c\x02\xff\xe0"),
			expectedVisibilityBitmap: case2VisibilityBitmap,
			expectedVisibilityMap:    case2VisibilityMap,
		},
		{
			// mariadb 10.4, case2
			data:                     []byte("\x12\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x0b_visibility\x00\x0b\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x00\xff\x07"),
			expectedVisibilityBitmap: []byte(nil),
			expectedVisibilityMap:    map[int]bool(nil),
		},
		{
			// mariadb 10.5, case2
			data:                     []byte("\x12\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x0b_visibility\x00\x0b\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x03\x00\xff\x07\x01\x02\x00\x00\x048\x04col0\x04col1\x04col2\x04col3\x04col4\x04col5\x04col6\x04col7\x04col8\x04col9\x05col10"),
			expectedVisibilityBitmap: []byte(nil),
			expectedVisibilityMap:    map[int]bool(nil),
		},
	}

	for _, tc := range testcases {
		tableMapEvent := new(TableMapEvent)
		tableMapEvent.tableIDSize = 6
		err := tableMapEvent.Decode(tc.data)
		require.NoError(t, err)
		require.Equal(t, tc.expectedVisibilityBitmap, tableMapEvent.VisibilityBitmap)
		require.Equal(t, tc.expectedVisibilityMap, tableMapEvent.VisibilityMap())
	}
}

func TestRowsDataExtraData(t *testing.T) {
	// Only after mysql 8.0.16 version can be parsed from extradata to 'partition info' and 'ndb info'
	testcases := []struct {
		data                    []byte
		tableData               []byte
		eventType               EventType
		expectPartitionId       uint16
		expectSourcePartitionId uint16
		expectNdbFormat         byte
		expectNdbData           []byte
	}{
		/*
			mysql-cluster 8.0.32

			+-------+------+------+-----+---------+-------+
			| Field | Type | Null | Key | Default | Extra |
			+-------+------+------+-----+---------+-------+
			| p     | int  | NO   | PRI | NULL    |       |
			| c     | int  | YES  | UNI | NULL    |       |
			+-------+------+------+-----+---------+-------+

			CREATE TABLE t (
				p INT PRIMARY KEY,
				c INT,
				UNIQUE KEY u (c)
			)   ENGINE NDB;

			INSERT INTO t VALUES (1,1), (2,2), (3,3), (4,4), (5,5);
		*/
		{
			data:                    []byte("s\x00\x00\x00\x00\x00\x01\x00\x0f\x00\x00\f\x00\x01\x00\x00\x04\x80\x00\x04\x00\x00\x00\x02\xff\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00\x02\x00\x00\x00\x02\x00\x00\x00\x00\x04\x00\x00\x00\x04\x00\x00\x00\x00\x03\x00\x00\x00\x03\x00\x00\x00\x00\x05\x00\x00\x00\x05\x00\x00\x00"),
			tableData:               []byte("s\x00\x00\x00\x00\x00\x01\x00\abdteste\x00\x01t\x00\x02\x03\x03\x00\x02\x01\x01\x00"),
			eventType:               WRITE_ROWS_EVENTv2,
			expectPartitionId:       0x0,
			expectSourcePartitionId: 0x0,
			expectNdbFormat:         0x0,
			expectNdbData:           []byte("\x01\x00\x00\x04\x80\x00\x04\x00\x00\x00"),
		},
		/*
				mysql 8.0.16

				+-------+------+------+-----+---------+-------+
				| Field | Type | Null | Key | Default | Extra |
				+-------+------+------+-----+---------+-------+
				| id    | int  | YES  |     | NULL    |       |
				+-------+------+------+-----+---------+-------+

				CREATE TABLE test (id INTEGER)
			            PARTITION BY RANGE (id) (
			                PARTITION p0 VALUES LESS THAN (1),
			                PARTITION p1 VALUES LESS THAN (2),
			                PARTITION p2 VALUES LESS THAN (3),
			                PARTITION p3 VALUES LESS THAN (4),
			                PARTITION p4 VALUES LESS THAN (5)
			            );

				INSERT INTO test (id) VALUES(3);
				UPDATE test set id = 1 WHERE id = 3;
		*/
		{
			data:                    []byte("p\x03\x00\x00\x00\x00\x01\x00\x05\x00\x01\x03\x00\x01\xff\x00\x03\x00\x00\x00"),
			tableData:               []byte("p\x03\x00\x00\x00\x00\x01\x00\x04test\x00\x04test\x00\x01\x03\x00\x01\x01\x01\x00"),
			eventType:               WRITE_ROWS_EVENTv2,
			expectPartitionId:       0x3,
			expectSourcePartitionId: 0x0,
			expectNdbFormat:         0x0,
			expectNdbData:           []byte(nil),
		},
		{
			data:                    []byte("p\x03\x00\x00\x00\x00\x01\x00\a\x00\x01\x01\x00\x03\x00\x01\xff\xff\x00\x03\x00\x00\x00\x00\x01\x00\x00\x00"),
			tableData:               []byte("p\x03\x00\x00\x00\x00\x01\x00\x04test\x00\x04test\x00\x01\x03\x00\x01\x01\x01\x00"),
			eventType:               UPDATE_ROWS_EVENTv2,
			expectPartitionId:       0x1,
			expectSourcePartitionId: 0x3,
			expectNdbFormat:         0x0,
			expectNdbData:           []byte(nil),
		},
		// mysql 5.7 and mariadb 14(15) does not surpot extra data
		{
			data:                    []byte("m\x00\x00\x00\x00\x00\x01\x00\x02\x00\x01\xff\xfe\x03\x00\x00\x00"),
			tableData:               []byte("m\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x04test\x00\x01\x03\x00\x01"),
			eventType:               WRITE_ROWS_EVENTv2,
			expectPartitionId:       0x0,
			expectSourcePartitionId: 0x0,
			expectNdbFormat:         0x0,
			expectNdbData:           []byte(nil),
		},
		{
			data:                    []byte("m\x00\x00\x00\x00\x00\x01\x00\x02\x00\x01\xff\xff\xfe\x03\x00\x00\x00\xfe\x01\x00\x00\x00"),
			tableData:               []byte("m\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x04test\x00\x01\x03\x00\x01"),
			eventType:               UPDATE_ROWS_EVENTv2,
			expectPartitionId:       0x0,
			expectSourcePartitionId: 0x0,
			expectNdbFormat:         0x0,
			expectNdbData:           []byte(nil),
		},
	}

	for _, tc := range testcases {
		tableMapEvent := new(TableMapEvent)
		tableMapEvent.tableIDSize = 6
		err := tableMapEvent.Decode(tc.tableData)
		require.NoError(t, err)

		rowsEvent := new(RowsEvent)
		rowsEvent.tableIDSize = 6
		rowsEvent.tables = make(map[uint64]*TableMapEvent)
		rowsEvent.tables[tableMapEvent.TableID] = tableMapEvent
		rowsEvent.Version = 2
		rowsEvent.eventType = tc.eventType

		err = rowsEvent.Decode(tc.data)
		require.NoError(t, err)
		require.Equal(t, tc.expectPartitionId, rowsEvent.PartitionId)
		require.Equal(t, tc.expectSourcePartitionId, rowsEvent.SourcePartitionId)
		require.Equal(t, tc.expectNdbFormat, rowsEvent.NdbFormat)
		require.Equal(t, tc.expectNdbData, rowsEvent.NdbData)
	}
}

func TestRowsEventType(t *testing.T) {
	testcases := []struct {
		eventType EventType
		want      EnumRowsEventType
	}{
		{WRITE_ROWS_EVENTv0, EnumRowsEventTypeInsert},
		{WRITE_ROWS_EVENTv1, EnumRowsEventTypeInsert},
		{WRITE_ROWS_EVENTv2, EnumRowsEventTypeInsert},
		{MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1, EnumRowsEventTypeInsert},
		{UPDATE_ROWS_EVENTv0, EnumRowsEventTypeUpdate},
		{UPDATE_ROWS_EVENTv1, EnumRowsEventTypeUpdate},
		{UPDATE_ROWS_EVENTv2, EnumRowsEventTypeUpdate},
		{MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1, EnumRowsEventTypeUpdate},
		{DELETE_ROWS_EVENTv0, EnumRowsEventTypeDelete},
		{DELETE_ROWS_EVENTv1, EnumRowsEventTypeDelete},
		{DELETE_ROWS_EVENTv2, EnumRowsEventTypeDelete},
		{MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1, EnumRowsEventTypeDelete},

		// Whoops, these are not rows events at all
		{EXEC_LOAD_EVENT, EnumRowsEventTypeUnknown},
		{HEARTBEAT_EVENT, EnumRowsEventTypeUnknown},
	}

	for _, tc := range testcases {
		rev := new(RowsEvent)
		rev.eventType = tc.eventType

		require.Equal(t, tc.want, rev.Type())
	}
}

func TestTableMapHelperMaps(t *testing.T) {
	/*
		CREATE TABLE `_types` (
			`b_bit` bit(64) NOT NULL DEFAULT b'0',

			`n_boolean` boolean not null default '0',
			`n_tinyint` tinyint not null default '0',
			`n_smallint` smallint not null default '0',
			`n_mediumint` mediumint not null default '0',
			`n_int` int not null default '0',
			`n_bigint` bigint not null default '0',
			`n_decimal` decimal(65,30) not null default '0',
			`n_float` float not null default '0',
			`n_double` double not null default '0',

			`nu_tinyint` tinyint unsigned not null default '0',
			`nu_smallint` smallint unsigned not null default '0',
			`nu_mediumint` mediumint unsigned not null default '0',
			`nu_int` int unsigned not null default '0',
			`nu_bigint` bigint unsigned not null default '0',
			`nu_decimal` decimal(65,30) unsigned not null default '0',
			`nu_float` float unsigned not null default '0',
			`nu_double` double unsigned not null default '0',

			`t_year` year default null,
			`t_date` date default null,
			`t_time` time default null,
			`t_ftime` time(6) default null,
			`t_datetime` datetime default null,
			`t_fdatetime` datetime(6) default null,
			`t_timestamp` timestamp default current_timestamp,
			`t_ftimestamp` timestamp(6) default current_timestamp(6),

			`c_char` char(255) collate gbk_chinese_ci not null default '',  -- collate id: 28
			`c_varchar` varchar(255) not null default '',
			`c_binary` binary(64) not null default '',
			`c_varbinary` varbinary(64) not null default '',
			`c_tinyblob` tinyblob,
			`c_blob` blob,
			`c_mediumblob` mediumblob,
			`c_longblob` longblob,
			`c_tinytext` tinytext,
			`c_text` text,
			`c_mediumtext` mediumtext,
			`c_longtext` longtext,

			`e_enum` enum('a','b') default 'a',
			`s_set` set('1','2') default '1',
			`g_geometry` geometry default null,
			`j_json` json default null,

			`s_set2` set('3','4') collate gbk_chinese_ci default '4',
			`e_enum2` enum('c','d') collate gbk_chinese_ci default 'd',
			`g_geometrycollection` geometrycollection default null,
			`g_multipolygon` multipolygon default null,
			`g_multilinestring` multilinestring default null,
			`g_multipoint` multipoint default null,
			`g_polygon` polygon default null,
			`g_linestring` linestring default null,
			`g_point` point default null
		);
	*/

	unsignedMap := map[int]bool{}
	for i := 1; i <= 9; i++ {
		unsignedMap[i] = false
	}
	for i := 10; i <= 17; i++ {
		unsignedMap[i] = true
	}

	// collation id | collatation
	//     28       | gbk_chinese_ci
	//     46       | utf8mb4_bin
	//     63       | binary
	//     224      | utf8mb4_unicode_ci
	mysqlCollationMap := map[int]uint64{
		26: 28, 27: 224, 28: 63, 29: 63, // (var)char/(var)binary
		30: 63, 31: 63, 32: 63, 33: 63, // blobs
		34: 224, 35: 224, 36: 224, 37: 224, // texts
	}
	// NOTE: mariadb treat json/geometry as character fields
	mariadbCollationMap := map[int]uint64{
		26: 28, 27: 224, 28: 63, 29: 63, // (var)char/(var)binary
		30: 63, 31: 63, 32: 63, 33: 63, // blobs
		34: 224, 35: 224, 36: 224, 37: 224, // texts
		40: 63,                                                 // geometry
		41: 46,                                                 // json
		44: 63, 45: 63, 46: 63, 47: 63, 48: 63, 49: 63, 50: 63, // geometry
	}

	enumSetCollationMap := map[int]uint64{
		38: 224, 39: 224, 42: 28, 43: 28,
	}

	enumStrValueMap := map[int][]string{
		38: {"a", "b"},
		43: {"c", "d"},
	}

	setStrValueMap := map[int][]string{
		39: {"1", "2"},
		42: {"3", "4"},
	}

	geometryTypeMap := map[int]uint64{
		40: 0,
		44: 7, 45: 6, 46: 5, 47: 4, 48: 3, 49: 2, 50: 1,
	}

	testcases := []struct {
		flavor              string
		data                []byte
		unsignedMap         map[int]bool
		collationMap        map[int]uint64
		enumSetCollationMap map[int]uint64
		enumStrValueMap     map[int][]string
		setStrValueMap      map[int][]string
		geometryTypeMap     map[int]uint64
	}{
		{
			flavor:              "mysql", // mysql 8.0
			data:                []byte("e\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x06_types\x003\x10\x01\x01\x02\t\x03\b\xf6\x04\x05\x01\x02\t\x03\b\xf6\x04\x05\r\n\x13\x13\x12\x12\x11\x11\xfe\x0f\xfe\x0f\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfe\xfe\xff\xf5\xfe\xfe\xff\xff\xff\xff\xff\xff\xff1\x00\bA\x1e\x04\bA\x1e\x04\b\x00\x06\x00\x06\x00\x06\xee\xfe\xfc\x03\xfe@@\x00\x01\x02\x03\x04\x01\x02\x03\x04\xf7\x01\xf8\x01\x04\x04\xf8\x01\xf7\x01\x04\x04\x04\x04\x04\x04\x04\x00\x00\xfc\xc3\xff\xff\a\x01\x03\x00\u007f\x80\x03\f\x1c\xe0??????\xe0\xe0\xe0\xe0\a\b\x00\a\x06\x05\x04\x03\x02\x01\x04\xfc\x05\x02\x05b_bit\tn_boolean\tn_tinyint\nn_smallint\vn_mediumint\x05n_int\bn_bigint\tn_decimal\an_float\bn_double\nnu_tinyint\vnu_smallint\fnu_mediumint\x06nu_int\tnu_bigint\nnu_decimal\bnu_float\tnu_double\x06t_year\x06t_date\x06t_time\at_ftime\nt_datetime\vt_fdatetime\vt_timestamp\ft_ftimestamp\x06c_char\tc_varchar\bc_binary\vc_varbinary\nc_tinyblob\x06c_blob\fc_mediumblob\nc_longblob\nc_tinytext\x06c_text\fc_mediumtext\nc_longtext\x06e_enum\x05s_set\ng_geometry\x06j_json\x06s_set2\ae_enum2\x14g_geometrycollection\x0eg_multipolygon\x11g_multilinestring\fg_multipoint\tg_polygon\fg_linestring\ag_point\v\x04\xe0\xe0\x1c\x1c\x05\n\x02\x011\x012\x02\x013\x014\x06\n\x02\x01a\x01b\x02\x01c\x01d"),
			unsignedMap:         unsignedMap,
			collationMap:        mysqlCollationMap,
			enumSetCollationMap: enumSetCollationMap,
			enumStrValueMap:     enumStrValueMap,
			setStrValueMap:      setStrValueMap,
			geometryTypeMap:     geometryTypeMap,
		},
		{
			flavor:              "mariadb", // mariadb 10.5
			data:                []byte("\x1e\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x06_types\x003\x10\x01\x01\x02\t\x03\b\xf6\x04\x05\x01\x02\t\x03\b\xf6\x04\x05\r\n\x13\x13\x12\x12\x11\x11\xfe\x0f\xfe\x0f\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfe\xfe\xff\xfc\xfe\xfe\xff\xff\xff\xff\xff\xff\xff1\x00\bA\x1e\x04\bA\x1e\x04\b\x00\x06\x00\x06\x00\x06\xee\xfe\xfc\x03\xfe@@\x00\x01\x02\x03\x04\x01\x02\x03\x04\xf7\x01\xf8\x01\x04\x04\xf8\x01\xf7\x01\x04\x04\x04\x04\x04\x04\x04\x00\x00\xfc\xc0\xff\xff\a\x01\x03\x00\u007f\xc0\x02\x0f?\x00\x1c\x01\xe0\b\xe0\t\xe0\n\xe0\v\xe0\r.\a\b\x00\a\x06\x05\x04\x03\x02\x01\x04\xfc\x05\x02\x05b_bit\tn_boolean\tn_tinyint\nn_smallint\vn_mediumint\x05n_int\bn_bigint\tn_decimal\an_float\bn_double\nnu_tinyint\vnu_smallint\fnu_mediumint\x06nu_int\tnu_bigint\nnu_decimal\bnu_float\tnu_double\x06t_year\x06t_date\x06t_time\at_ftime\nt_datetime\vt_fdatetime\vt_timestamp\ft_ftimestamp\x06c_char\tc_varchar\bc_binary\vc_varbinary\nc_tinyblob\x06c_blob\fc_mediumblob\nc_longblob\nc_tinytext\x06c_text\fc_mediumtext\nc_longtext\x06e_enum\x05s_set\ng_geometry\x06j_json\x06s_set2\ae_enum2\x14g_geometrycollection\x0eg_multipolygon\x11g_multilinestring\fg_multipoint\tg_polygon\fg_linestring\ag_point\v\x04\xe0\xe0\x1c\x1c\x05\n\x02\x011\x012\x02\x013\x014\x06\n\x02\x01a\x01b\x02\x01c\x01d"),
			unsignedMap:         unsignedMap,
			collationMap:        mariadbCollationMap,
			enumSetCollationMap: enumSetCollationMap,
			enumStrValueMap:     enumStrValueMap,
			setStrValueMap:      setStrValueMap,
			geometryTypeMap:     geometryTypeMap,
		},
		{
			flavor:              "mysql", // mysql 5.7
			data:                []byte("q\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x06_types\x003\x10\x01\x01\x02\t\x03\b\xf6\x04\x05\x01\x02\t\x03\b\xf6\x04\x05\r\n\x13\x13\x12\x12\x11\x11\xfe\x0f\xfe\x0f\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfe\xfe\xff\xf5\xfe\xfe\xff\xff\xff\xff\xff\xff\xff1\x00\bA\x1e\x04\bA\x1e\x04\b\x00\x06\x00\x06\x00\x06\xee\xfe\xfc\x03\xfe@@\x00\x01\x02\x03\x04\x01\x02\x03\x04\xf7\x01\xf8\x01\x04\x04\xf8\x01\xf7\x01\x04\x04\x04\x04\x04\x04\x04\x00\x00\xfc\xc0\xff\xff\a"),
			unsignedMap:         nil,
			collationMap:        nil,
			enumSetCollationMap: nil,
			enumStrValueMap:     nil,
			setStrValueMap:      nil,
			geometryTypeMap:     nil,
		},
		{
			flavor:              "mariadb", // mariadb 10.4
			data:                []byte("\x1a\x00\x00\x00\x00\x00\x01\x00\x04test\x00\x06_types\x003\x10\x01\x01\x02\t\x03\b\xf6\x04\x05\x01\x02\t\x03\b\xf6\x04\x05\r\n\x13\x13\x12\x12\x11\x11\xfe\x0f\xfe\x0f\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfc\xfe\xfe\xff\xfc\xfe\xfe\xff\xff\xff\xff\xff\xff\xff1\x00\bA\x1e\x04\bA\x1e\x04\b\x00\x06\x00\x06\x00\x06\xee\xfe\xfc\x03\xfe@@\x00\x01\x02\x03\x04\x01\x02\x03\x04\xf7\x01\xf8\x01\x04\x04\xf8\x01\xf7\x01\x04\x04\x04\x04\x04\x04\x04\x00\x00\xfc\xc0\xff\xff\a"),
			unsignedMap:         nil,
			collationMap:        nil,
			enumSetCollationMap: nil,
			enumStrValueMap:     nil,
			setStrValueMap:      nil,
			geometryTypeMap:     nil,
		},
	}

	for _, tc := range testcases {
		tableMapEvent := new(TableMapEvent)
		tableMapEvent.flavor = tc.flavor
		tableMapEvent.tableIDSize = 6
		err := tableMapEvent.Decode(tc.data)
		require.NoError(t, err)
		require.Equal(t, tc.unsignedMap, tableMapEvent.UnsignedMap())
		require.Equal(t, tc.collationMap, tableMapEvent.CollationMap())
		require.Equal(t, tc.enumSetCollationMap, tableMapEvent.EnumSetCollationMap())
		require.Equal(t, tc.enumStrValueMap, tableMapEvent.EnumStrValueMap())
		require.Equal(t, tc.setStrValueMap, tableMapEvent.SetStrValueMap())
		require.Equal(t, tc.geometryTypeMap, tableMapEvent.GeometryTypeMap())
	}
}

func TestInvalidEvent(t *testing.T) {
	data := "@\x01\x00\x00\x00\x00\x01\x00\x02\xff\xfc\x01\x00\x00\x00\x00B\x14U\x16\x8ew"
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     0x140,
		Flags:       0x1,
		Schema:      []uint8{0x74, 0x65, 0x73, 0x74},
		Table:       []uint8{0x74},
		ColumnCount: 0x2,
		ColumnType:  []uint8{0x3, 0xc},
		ColumnMeta:  []uint16{0x0, 0x0},
		NullBitmap:  []uint8{0x2},
	}

	e2 := &RowsEvent{
		Version:     1,
		tableIDSize: 6,
	}
	e2.tables = map[uint64]*TableMapEvent{}
	e2.tables[0x140] = table
	err := e2.Decode([]byte(data))
	require.Error(t, err)
}

func TestDecodeTime2(t *testing.T) {
	testcases := []struct {
		data        []byte
		dec         uint16
		getFracTime bool
		expected    string
	}{
		{[]byte("\xb4\x6e\xfb"), 0, false, "838:59:59"},
		{[]byte("\x80\xf1\x05"), 0, false, "15:04:05"},
		{[]byte("\x80\x00\x00"), 0, false, "00:00:00"},
		{[]byte("\x7f\xff\xff"), 0, false, "-00:00:01"},
		{[]byte("\x7f\x0e\xfb"), 0, false, "-15:04:05"},
		{[]byte("\x4b\x91\x05"), 0, false, "-838:59:59"},
		{[]byte("\x7f\xff\xff\xff"), 2, true, "-00:00:00.01"},
		{[]byte("\x7f\x0e\xfa\xf4"), 2, true, "-15:04:05.12"},
		{[]byte("\x4b\x91\x05\xf4"), 2, true, "-838:59:58.12"},
		{[]byte("\x7f\xff\xff\xff\xff"), 4, true, "-00:00:00.0001"},
		{[]byte("\x7f\x0e\xfa\xfb\x2d"), 4, true, "-15:04:05.1235"},
		{[]byte("\x4b\x91\x05\xfb\x2d"), 4, true, "-838:59:58.1235"},
		{[]byte("\x7f\xff\xff\xff\xff\xff"), 6, true, "-00:00:00.000001"},
		{[]byte("\x7f\x0e\xfa\xfe\x1d\xc0"), 6, true, "-15:04:05.123456"},
		{[]byte("\x4b\x91\x05\xfe\x1d\xc0"), 6, true, "-838:59:58.123456"},
	}
	for _, tc := range testcases {
		value, _, err := decodeTime2(tc.data, tc.dec)
		require.NoError(t, err)
		require.Equal(t, tc.expected, value)
	}
}

type decimalTest struct {
	num      string
	dumpData []byte
	meta     uint16
}

var decimalData = []decimalTest{
	// DECIMAL(40, 16)
	{
		"123.4560000000000000",
		[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 123, 27, 46, 2, 0, 0, 0, 0, 0},
		10256,
	},
	{
		"0.0000010000000000",
		[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 232, 0, 0, 0, 0},
		10256,
	},
	{
		"100000000.0000000000000000",
		[]byte{128, 0, 0, 0, 0, 0, 0, 5, 245, 225, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		10256,
	},
	{
		"100000000.0000000200000000",
		[]byte{128, 0, 0, 0, 0, 0, 0, 5, 245, 225, 0, 0, 0, 0, 20, 0, 0, 0, 0},
		10256,
	},
	{
		"123456.1234567890000000",
		[]byte{128, 0, 0, 0, 0, 0, 0, 0, 1, 226, 64, 7, 91, 205, 21, 0, 0, 0, 0},
		10256,
	},
	{
		"123456234234234757655.1234567890123456",
		[]byte{128, 0, 123, 27, 49, 148, 250, 13, 254, 30, 23, 7, 91, 205, 21, 0, 1, 226, 64},
		10256,
	},
	{
		"-123456234234234757655.1234567890123456",
		[]byte{127, 255, 132, 228, 206, 107, 5, 242, 1, 225, 232, 248, 164, 50, 234, 255, 254, 29, 191},
		10256,
	},
	{
		"0.0000000000000000",
		[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		10256,
	},
	// DECIMAL(60, 0)
	{
		"1000000000000000000000000000000",
		[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 232, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		15360,
	},
	{
		"1",
		[]byte{128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		15360,
	},
	// DECIMAL(30, 30)
	{
		"0.100000000000000000000000000000",
		[]byte{133, 245, 225, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		7710,
	},
	{
		"0.000000000000001000000000000000",
		[]byte{128, 0, 0, 0, 0, 0, 3, 232, 0, 0, 0, 0, 0, 0},
		7710,
	},
}

func BenchmarkUseDecimal(b *testing.B) {
	e := &RowsEvent{useDecimal: true}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, d := range decimalData {
			_, _, _ = e.decodeValue(d.dumpData, mysql.MYSQL_TYPE_NEWDECIMAL, d.meta, false)
		}
	}
}

func BenchmarkNotUseDecimal(b *testing.B) {
	e := &RowsEvent{useDecimal: false}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, d := range decimalData {
			_, _, _ = e.decodeValue(d.dumpData, mysql.MYSQL_TYPE_NEWDECIMAL, d.meta, false)
		}
	}
}

func TestDecimal(t *testing.T) {
	e := &RowsEvent{useDecimal: true}
	e2 := &RowsEvent{useDecimal: false}
	for _, d := range decimalData {
		v, _, err := e.decodeValue(d.dumpData, mysql.MYSQL_TYPE_NEWDECIMAL, d.meta, false)
		require.NoError(t, err)
		// no trailing zero
		dec, err := decimal.NewFromString(d.num)
		require.NoError(t, err)
		require.True(t, dec.Equal(v.(decimal.Decimal)))

		v, _, err = e2.decodeValue(d.dumpData, mysql.MYSQL_TYPE_NEWDECIMAL, d.meta, false)
		require.NoError(t, err)
		require.Equal(t, d.num, v.(string))
	}
}

var intData = [][]byte{
	{1, 0, 0, 0},
	{2, 0, 0, 0},
	{3, 0, 0, 0},
	{4, 0, 0, 0},
	{5, 0, 0, 0},
	{6, 0, 0, 0},
	{7, 0, 0, 0},
	{8, 0, 0, 0},
	{9, 0, 0, 0},
	{10, 0, 0, 0},
	{11, 0, 0, 0},
	{12, 0, 0, 0},
}

func BenchmarkInt(b *testing.B) {
	e := &RowsEvent{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, d := range intData {
			_, _, _ = e.decodeValue(d, mysql.MYSQL_TYPE_LONG, 0, false)
		}
	}
}
