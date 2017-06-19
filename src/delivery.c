/*
 * -*-C-*-
 * delivery.pc
 * corresponds to A.4 in appendix A
 */

#include <stdio.h>
#include <string.h>
#include <time.h>

#include <mysql.h>

#include "spt_proc.h"
#include "tpc.h"

extern MYSQL **ctx;
extern MYSQL_STMT ***stmt;

#define NNULL ((void *)0)

const char* sql26 = "SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = ? AND no_w_id = ?";
const char* sql27 = "DELETE FROM new_orders WHERE no_o_id = ? AND no_d_id = ? AND no_w_id = ?";
const char* sql28 = "SELECT o_c_id FROM orders WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?";
const char* sql29 = "UPDATE orders SET o_carrier_id = ? WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?";
const char* sql30 = "UPDATE order_line SET ol_delivery_d = ? WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?";
const char* sql31 = "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?";
const char* sql32 = "UPDATE customer SET c_balance = c_balance + ? , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?";
int delivery( int t_num,
	      int w_id_arg,
	      int o_carrier_id_arg
)
{
	int            w_id = w_id_arg;
	int            o_carrier_id = o_carrier_id_arg;
	int            d_id;
	int            c_id;
	int            no_o_id;
	float           ol_total;
	char            datetime[81];

	int proceed = 0;
  MYSQL* mysql = ctx[t_num];
  MYSQL_RES* result;
  MYSQL_ROW row;
	MYSQL_STMT*   mysql_stmt;
        MYSQL_BIND    param[4];
        MYSQL_BIND    column[1];

	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

        gettimestamp(datetime, STRFTIME_FORMAT, TIMESTAMP_LEN);

	/* For each district in warehouse */
	/* printf("W: %d\n", w_id); */

  char para10[10][10000];
  char executed_sql[50000];
	for (d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
	        proceed = 1;
		/*EXEC_SQL SELECT COALESCE(MIN(no_o_id),0) INTO :no_o_id
		                FROM new_orders
		                WHERE no_d_id = :d_id AND no_w_id = :w_id;*/
		mysql_stmt = stmt[t_num][25];

  sprintf(para10[0], "%d", d_id);
  sprintf(para10[1], "%d", w_id);
  sprintf(executed_sql, "%s", sql26);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      no_o_id = atoi(row[0]);
  }
  mysql_free_result(result);

		if(no_o_id == 0) continue;
		proceed = 2;
		/*EXEC_SQL DELETE FROM new_orders WHERE no_o_id = :no_o_id AND no_d_id = :d_id
		  AND no_w_id = :w_id;*/
		mysql_stmt = stmt[t_num][26];

    sprintf(para10[0], "%d", no_o_id);
    sprintf(para10[1], "%d", d_id);
    sprintf(para10[2], "%d", w_id);
    sprintf(executed_sql, "%s", sql27);
    replace_the_para(executed_sql, para10);
    if (mysql_query(mysql, executed_sql)) goto sqlerr;


		proceed = 3;
		/*EXEC_SQL SELECT o_c_id INTO :c_id FROM orders
		                WHERE o_id = :no_o_id AND o_d_id = :d_id
				AND o_w_id = :w_id;*/
		mysql_stmt = stmt[t_num][27];

  sprintf(para10[0], "%d", no_o_id);
  sprintf(para10[1], "%d", d_id);
  sprintf(para10[2], "%d", w_id);
  sprintf(executed_sql, "%s", sql28);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      c_id = atoi(row[0]);
  }
  mysql_free_result(result);

		proceed = 4;
		/*EXEC_SQL UPDATE orders SET o_carrier_id = :o_carrier_id
		                WHERE o_id = :no_o_id AND o_d_id = :d_id AND
				o_w_id = :w_id;*/
		mysql_stmt = stmt[t_num][28];

  sprintf(para10[0], "%d", o_carrier_id);
  sprintf(para10[1], "%d", no_o_id);
  sprintf(para10[2], "%d", d_id);
  sprintf(para10[3], "%d", w_id);
  sprintf(executed_sql, "%s", sql29);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

		proceed = 5;
		/*EXEC_SQL UPDATE order_line
		                SET ol_delivery_d = :datetime
		                WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND
				ol_w_id = :w_id;*/
		mysql_stmt = stmt[t_num][29];

  sprintf(para10[0], "'%s'", datetime);
  sprintf(para10[1], "%d", no_o_id);
  sprintf(para10[2], "%d", d_id);
  sprintf(para10[3], "%d", w_id);
  sprintf(executed_sql, "%s", sql30);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

		proceed = 6;
		/*EXEC_SQL SELECT SUM(ol_amount) INTO :ol_total
		                FROM order_line
		                WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id
				AND ol_w_id = :w_id;*/
		mysql_stmt = stmt[t_num][30];

  sprintf(para10[0], "%d", no_o_id);
  sprintf(para10[1], "%d", d_id);
  sprintf(para10[2], "%d", w_id);
  sprintf(executed_sql, "%s", sql31);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      ol_total = strtod(row[0], NULL);
  }
  mysql_free_result(result);

		proceed = 7;
		/*EXEC_SQL UPDATE customer SET c_balance = c_balance + :ol_total ,
		                             c_delivery_cnt = c_delivery_cnt + 1
		                WHERE c_id = :c_id AND c_d_id = :d_id AND
				c_w_id = :w_id;*/
		mysql_stmt = stmt[t_num][31];

  sprintf(para10[0], "%f", ol_total);
  sprintf(para10[1], "%d", c_id);
  sprintf(para10[2], "%d", d_id);
  sprintf(para10[3], "%d", w_id);
  sprintf(executed_sql, "%s", sql32);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

		/*EXEC_SQL COMMIT WORK;*/
		if( mysql_commit(ctx[t_num]) ) goto sqlerr;

		/* printf("D: %d, O: %d, time: %d\n", d_id, o_id, tad); */

	}
	/*EXEC_SQL COMMIT WORK;*/
	return (1);

sqlerr:
        fprintf(stderr, "delivery %d:%d\n",t_num,proceed);
	error(ctx[t_num],mysql_stmt);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);
sqlerrerr:
	return (0);
}
