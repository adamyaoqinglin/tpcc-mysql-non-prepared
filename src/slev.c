/*
 * -*-C-*-
 * slev.pc 
 * corresponds to A.5 in appendix A
 */

#include <string.h>
#include <stdio.h>

#include <mysql.h>

#include "spt_proc.h"
#include "tpc.h"

extern MYSQL **ctx;
extern MYSQL_STMT ***stmt;

const char* sql33 = "SELECT d_next_o_id FROM district WHERE d_id = ? AND d_w_id = ?";
const char* sql34 = "SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= (? - 20)";
const char* sql35 = "SELECT count(*) FROM stock WHERE s_w_id = ? AND s_i_id = ? AND s_quantity < ?";
/*
 * the stock level transaction
 */
int slev( int t_num,
	  int w_id_arg,		/* warehouse id */
	  int d_id_arg,		/* district id */
	  int level_arg		/* stock level */
)
{
	int            w_id = w_id_arg;
	int            d_id = d_id_arg;
	int            level = level_arg;
	int            d_next_o_id;
	int            i_count;
	int            ol_i_id;

	MYSQL_STMT*   mysql_stmt;
        MYSQL_BIND    param[4];
        MYSQL_BIND    column[1];
	MYSQL_STMT*   mysql_stmt2;
        MYSQL_BIND    param2[3];
        MYSQL_BIND    column2[1];

  MYSQL * mysql = ctx[t_num];
  MYSQL_RES* result;
  MYSQL_ROW row;
  char para10[10][10000];
  char executed_sql[50000];
	/*EXEC SQL WHENEVER NOT FOUND GOTO sqlerr;*/
	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	/* find the next order id */
#ifdef DEBUG
	printf("select 1\n");
#endif
	/*EXEC_SQL SELECT d_next_o_id
	                INTO :d_next_o_id
	                FROM district
	                WHERE d_id = :d_id
			AND d_w_id = :w_id;*/
	mysql_stmt = stmt[t_num][32];

  sprintf(para10[0], "%d", d_id);
  sprintf(para10[1], "%d", w_id);
  sprintf(executed_sql, "%s", sql33);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      d_next_o_id = atoi(row[0]);
  }
  mysql_free_result(result);

	/* find the most recent 20 orders for this district */
	/*EXEC_SQL DECLARE ord_line CURSOR FOR
	                SELECT DISTINCT ol_i_id
	                FROM order_line
	                WHERE ol_w_id = :w_id
			AND ol_d_id = :d_id
			AND ol_o_id < :d_next_o_id
			AND ol_o_id >= (:d_next_o_id - 20);

	EXEC_SQL OPEN ord_line;

	EXEC SQL WHENEVER NOT FOUND GOTO done;*/
	mysql_stmt = stmt[t_num][33];

  sprintf(para10[0], "%d", w_id);
  sprintf(para10[1], "%d", d_id);
  sprintf(para10[2], "%d", d_next_o_id);
  sprintf(para10[3], "%d", d_next_o_id);
  sprintf(executed_sql, "%s", sql34);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      ol_i_id = atoi(row[0]);
  }
  mysql_free_result(result);

	for (;;) {
#ifdef DEBUG
		printf("fetch 1\n");
#endif
		/*EXEC_SQL FETCH ord_line INTO :ol_i_id;*/

#ifdef DEBUG
		printf("select 2\n");
#endif

		/*EXEC_SQL SELECT count(*) INTO :i_count
			FROM stock
			WHERE s_w_id = :w_id
		        AND s_i_id = :ol_i_id
			AND s_quantity < :level;*/
		mysql_stmt2 = stmt[t_num][34];

  sprintf(para10[0], "%d", w_id);
  sprintf(para10[1], "%d", ol_i_id);
  sprintf(para10[2], "%d", level);
  sprintf(executed_sql, "%s", sql35);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      i_count = atoi(row[0]);
  }
  mysql_free_result(result);
  goto done;

	}

done:
	/*EXEC_SQL CLOSE ord_line;*/
	/*EXEC_SQL COMMIT WORK;*/
	if( mysql_commit(ctx[t_num]) ) goto sqlerr;

	return (1);

sqlerr:
        fprintf(stderr,"slev\n");
	error(ctx[t_num],mysql_stmt);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);
sqlerrerr:
	return (0);

sqlerr2:
        fprintf(stderr,"slev\n");
	error(ctx[t_num],mysql_stmt2);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_stmt_free_result(mysql_stmt);
	mysql_rollback(ctx[t_num]);
	return (0);
}
