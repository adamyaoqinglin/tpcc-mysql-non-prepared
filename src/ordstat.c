/*
 * -*-C-*- 
 * ordstat.pc 
 * corresponds to A.3 in appendix A
 */

#include <string.h>
#include <stdio.h>

#include <mysql.h>

#include "spt_proc.h"
#include "tpc.h"

extern MYSQL **ctx;
extern MYSQL_STMT ***stmt;

const char* sql21 = "SELECT count(c_id) FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?";
const char* sql22 = "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first";
const char* sql23 = "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";
const char* sql24 = "SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = (SELECT MAX(o_id) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?)";
const char* sql25 = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?";
/*
 * the order status transaction
 */
int ordstat( int t_num,
	     int w_id_arg,		/* warehouse id */
	     int d_id_arg,		/* district id */
	     int byname,		/* select by c_id or c_last? */
	     int c_id_arg,		/* customer id */
	     char c_last_arg[]	        /* customer last name, format? */
)
{
	int            w_id = w_id_arg;
	int            d_id = d_id_arg;
	int            c_id = c_id_arg;
	int            c_d_id = d_id;
	int            c_w_id = w_id;
	char            c_first[17];
	char            c_middle[3];
	char            c_last[17];
	float           c_balance;
	int            o_id;
	char            o_entry_d[25];
	int            o_carrier_id;
	int            ol_i_id;
	int            ol_supply_w_id;
	int            ol_quantity;
	float           ol_amount;
	char            ol_delivery_d[25];
	int            namecnt;

	int             n;
	int             proceed = 0;

	MYSQL_STMT*   mysql_stmt;
        MYSQL_BIND    param[6];
        MYSQL_BIND    column[5];

	/*EXEC SQL WHENEVER NOT FOUND GOTO sqlerr;*/
	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

  MYSQL * mysql = ctx[t_num];
  MYSQL_RES* result;
  MYSQL_ROW row;
  char para10[10][10000];
  char executed_sql[50000];
	if (byname) {
		strcpy(c_last, c_last_arg);
		proceed = 1;
		/*EXEC_SQL SELECT count(c_id)
			INTO :namecnt
		        FROM customer
			WHERE c_w_id = :c_w_id
			AND c_d_id = :c_d_id
		        AND c_last = :c_last;*/
		mysql_stmt = stmt[t_num][20];

  sprintf(para10[0], "%d", c_w_id);
  sprintf(para10[1], "%d", c_d_id);
  sprintf(para10[2], "'%s'", c_last);
  sprintf(executed_sql, "%s", sql21);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      namecnt = atoi(row[0]);
  }
  mysql_free_result(result);

		proceed = 2;
		/*EXEC_SQL DECLARE c_byname_o CURSOR FOR
		        SELECT c_balance, c_first, c_middle, c_last
		        FROM customer
		        WHERE c_w_id = :c_w_id
			AND c_d_id = :c_d_id
			AND c_last = :c_last
			ORDER BY c_first;
		proceed = 3;
		EXEC_SQL OPEN c_byname_o;*/
		mysql_stmt = stmt[t_num][21];

  sprintf(para10[0], "%d", c_w_id);
  sprintf(para10[1], "%d", c_d_id);
  sprintf(para10[2], "'%s'", c_last);
  sprintf(executed_sql, "%s", sql22);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      c_balance = strtod(row[0], NULL);
    if (row[1])
      sprintf(c_first, "%s", row[1]);
    if (row[2])
      sprintf(c_middle, "%s", row[2]);
    if (row[3])
      sprintf(c_last, "%s", row[3]);
  }
  mysql_free_result(result);

		if (namecnt % 2)
			namecnt++;	/* Locate midpoint customer; */

		proceed = 5;


	} else {		/* by number */
		proceed = 6;
		/*EXEC_SQL SELECT c_balance, c_first, c_middle, c_last
			INTO :c_balance, :c_first, :c_middle, :c_last
		        FROM customer
		        WHERE c_w_id = :c_w_id
			AND c_d_id = :c_d_id
			AND c_id = :c_id;*/
		mysql_stmt = stmt[t_num][22];

  sprintf(para10[0], "%d", c_w_id);
  sprintf(para10[1], "%d", c_d_id);
  sprintf(para10[2], "%d", c_id);
  sprintf(executed_sql, "%s", sql23);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      c_balance = strtod(row[0], NULL);
    if (row[1])
      sprintf(c_first, "%s", row[1]);
    if (row[2])
      sprintf(c_middle, "%s", row[2]);
    if (row[3])
      sprintf(c_last, "%s", row[3]);
  }
  mysql_free_result(result);

	}

	/* find the most recent order for this customer */

	proceed = 7;
	/*EXEC_SQL SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0)
		INTO :o_id, :o_entry_d, :o_carrier_id
	        FROM orders
	        WHERE o_w_id = :c_w_id
		AND o_d_id = :c_d_id
		AND o_c_id = :c_id
		AND o_id = (SELECT MAX(o_id)
		    	    FROM orders
		    	    WHERE o_w_id = :c_w_id
		  	    AND o_d_id = :c_d_id
		    	    AND o_c_id = :c_id);*/
	mysql_stmt = stmt[t_num][23];

  sprintf(para10[0], "%d", c_w_id);
  sprintf(para10[1], "%d", c_d_id);
  sprintf(para10[2], "%d", c_id);
  sprintf(para10[3], "%d", c_w_id);
  sprintf(para10[4], "%d", c_d_id);
  sprintf(para10[5], "%d", c_id);
  sprintf(executed_sql, "%s", sql24);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      o_id = atoi(row[0]);
    if (row[1])
      sprintf(o_entry_d, "%s", row[1]);
    if (row[2])
      o_carrier_id = atoi(row[2]);
  }
  mysql_free_result(result);

	/* find all the items in this order */
	proceed = 8;
	/*EXEC_SQL DECLARE c_items CURSOR FOR
		SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount,
                       ol_delivery_d
		FROM order_line
	        WHERE ol_w_id = :c_w_id
		AND ol_d_id = :c_d_id
		AND ol_o_id = :o_id;*/
	mysql_stmt = stmt[t_num][24];

  sprintf(para10[0], "%d", c_w_id);
  sprintf(para10[1], "%d", c_d_id);
  sprintf(para10[2], "%d", o_id);
  sprintf(executed_sql, "%s", sql25);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      ol_i_id = atoi(row[0]);
    if (row[1])
      ol_supply_w_id = atoi(row[1]);
    if (row[2])
      ol_quantity = atoi(row[2]);
    if (row[3])
      ol_amount = strtod(row[3], NULL);
    if (row[4])
      sprintf(ol_delivery_d, "%s", row[4]);
  }
  mysql_free_result(result);

done:
	/*EXEC_SQL CLOSE c_items;*/
        /*EXEC_SQL COMMIT WORK;*/
	if( mysql_commit(ctx[t_num]) ) goto sqlerr;

	return (1);

sqlerr:
        fprintf(stderr, "ordstat %d:%d\n",t_num,proceed);
	error(ctx[t_num],mysql_stmt);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);
sqlerrerr:
	return (0);
}

