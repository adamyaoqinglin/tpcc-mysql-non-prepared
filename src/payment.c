/*
 * -*-C-*-  
 * payment.pc 
 * corresponds to A.2 in appendix A
 */

#include <string.h>
#include <stdio.h>
#include <time.h>

#include <mysql.h>

#include "spt_proc.h"
#include "tpc.h"


const char* sql10 = "UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?";
const char* sql11 = "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?";
const char* sql12 = "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?";
const char* sql13 = "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?";
const char* sql14 = "SELECT count(c_id) FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?";
const char* sql15 = "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first";
const char* sql16 = "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? FOR UPDATE";
const char* sql17 = "SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";
const char* sql18 = "UPDATE customer SET c_balance = ?, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";
const char* sql19 = "UPDATE customer SET c_balance = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";
const char* sql20 = "INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES(?, ?, ?, ?, ?, ?, ?, ?)";

extern MYSQL **ctx;
extern MYSQL_STMT ***stmt;

#define NNULL ((void *)0)

/*
 * the payment transaction
 */
int payment( int t_num,
	     int w_id_arg,		/* warehouse id */
	     int d_id_arg,		/* district id */
	     int byname,		/* select by c_id or c_last? */
	     int c_w_id_arg,
	     int c_d_id_arg,
	     int c_id_arg,		/* customer id */
	     char c_last_arg[],	        /* customer last name */
	     float h_amount_arg	        /* payment amount */
)
{
	int            w_id = w_id_arg;
	int            d_id = d_id_arg;
	int            c_id = c_id_arg;
	char            w_name[11];
	char            w_street_1[21];
	char            w_street_2[21];
	char            w_city[21];
	char            w_state[3];
	char            w_zip[10];
	int            c_d_id = c_d_id_arg;
	int            c_w_id = c_w_id_arg;
	char            c_first[17];
	char            c_middle[3];
	char            c_last[17];
	char            c_street_1[21];
	char            c_street_2[21];
	char            c_city[21];
	char            c_state[3];
	char            c_zip[10];
	char            c_phone[17];
	char            c_since[20];
	char            c_credit[4];
	int            c_credit_lim;
	float           c_discount;
	float           c_balance;
	char            c_data[502];
	char            c_new_data[502];
	float           h_amount = h_amount_arg;
	char            h_data[26];
	char            d_name[11];
	char            d_street_1[21];
	char            d_street_2[21];
	char            d_city[21];
	char            d_state[3];
	char            d_zip[10];
	int            namecnt;
	char            datetime[81];

	int             n;
	int             proceed = 0;

  MYSQL* mysql = ctx[t_num];
	MYSQL_STMT*   mysql_stmt;
        MYSQL_BIND    param[8];
        MYSQL_BIND    column[14];

  MYSQL_RES* result;
  MYSQL_ROW row;
	/* EXEC SQL WHENEVER NOT FOUND GOTO sqlerr; */
	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr; */

	gettimestamp(datetime, STRFTIME_FORMAT, TIMESTAMP_LEN);

	proceed = 1;
	/*EXEC_SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
	  WHERE w_id =:w_id;*/
	mysql_stmt = stmt[t_num][9];

  char para10[10][10000];
  sprintf(para10[0], "%f", h_amount);
  sprintf(para10[1], "%d", w_id);
  char executed_sql[50000];
  sprintf(executed_sql, "%s", sql10);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

/*	memset(param, 0, sizeof(MYSQL_BIND) * 2);
	param[0].buffer_type = MYSQL_TYPE_FLOAT;
        param[0].buffer = &h_amount;
        param[1].buffer_type = MYSQL_TYPE_LONG;
        param[1].buffer = &w_id;
	if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
        if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;
    */


	proceed = 2;
	/*EXEC_SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip,
	                w_name
	                INTO :w_street_1, :w_street_2, :w_city, :w_state,
				:w_zip, :w_name
	                FROM warehouse
	                WHERE w_id = :w_id;*/
	mysql_stmt = stmt[t_num][10];

  sprintf(para10[0], "%d", w_id);
  sprintf(executed_sql, "%s", sql11);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;
  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      sprintf(w_street_1, "%s", row[0]);
    if (row[1])
      sprintf(w_street_2, "%s", row[1]);
    if (row[2])
      sprintf(w_city, "%s", row[2]);
    if (row[3])
      sprintf(w_state, "%s", row[3]);
    if (row[4])
      sprintf(w_zip, "%s", row[4]);
    if (row[5])
      sprintf(w_name, "%s", row[5]);
  }
  mysql_free_result(result);
/*
	memset(param, 0, sizeof(MYSQL_BIND) * 1); 
	param[0].buffer_type = MYSQL_TYPE_LONG;
        param[0].buffer = &w_id;
	if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
        if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;

	if( mysql_stmt_store_result(mysql_stmt) ) goto sqlerr;
        memset(column, 0, sizeof(MYSQL_BIND) * 6);
	column[0].buffer_type = MYSQL_TYPE_STRING;
        column[0].buffer = w_street_1;
        column[0].buffer_length = sizeof(w_street_1);
	column[1].buffer_type = MYSQL_TYPE_STRING;
        column[1].buffer = w_street_2;
        column[1].buffer_length = sizeof(w_street_2);
	column[2].buffer_type = MYSQL_TYPE_STRING;
        column[2].buffer = w_city;
        column[2].buffer_length = sizeof(w_city);
	column[3].buffer_type = MYSQL_TYPE_STRING;
        column[3].buffer = w_state;
        column[3].buffer_length = sizeof(w_state);
	column[4].buffer_type = MYSQL_TYPE_STRING;
        column[4].buffer = w_zip;
        column[4].buffer_length = sizeof(w_zip);
	column[5].buffer_type = MYSQL_TYPE_STRING;
        column[5].buffer = w_name;
        column[5].buffer_length = sizeof(w_name);
	if( mysql_stmt_bind_result(mysql_stmt, column) ) goto sqlerr;
        switch( mysql_stmt_fetch(mysql_stmt) ) {
            case 0: //SUCCESS
                break;
            case 1: //ERROR
            case MYSQL_NO_DATA: //NO MORE DATA
            default:
                mysql_stmt_free_result(mysql_stmt);
                goto sqlerr;
        }
        mysql_stmt_free_result(mysql_stmt);

*/
	proceed = 3;
	/*EXEC_SQL UPDATE district SET d_ytd = d_ytd + :h_amount
			WHERE d_w_id = :w_id 
			AND d_id = :d_id;*/
	mysql_stmt = stmt[t_num][11];

  sprintf(para10[0], "%f", h_amount);
  sprintf(para10[1], "%d", w_id);
  sprintf(para10[2], "%d", d_id);
  sprintf(executed_sql, "%s", sql12);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;


	proceed = 4;
	/*EXEC_SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip,
	                d_name
	                INTO :d_street_1, :d_street_2, :d_city, :d_state,
				:d_zip, :d_name
	                FROM district
	                WHERE d_w_id = :w_id 
			AND d_id = :d_id;*/
	mysql_stmt = stmt[t_num][12];

  sprintf(para10[0], "%d", w_id);
  sprintf(para10[1], "%d", d_id);
  sprintf(executed_sql, "%s", sql13);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;
  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      sprintf(d_street_1, "%s", row[0]);
    if (row[1])
      sprintf(d_street_2, "%s", row[1]);
    if (row[2])
      sprintf(d_city, "%s", row[2]);
    if (row[3])
      sprintf(d_state, "%s", row[3]);
    if (row[4])
      sprintf(d_zip, "%s", row[4]);
    if (row[5])
      sprintf(d_name, "%s", row[5]);
  }
  mysql_free_result(result);


	if (byname) {
		strcpy(c_last, c_last_arg);

		proceed = 5;
		/*EXEC_SQL SELECT count(c_id) 
			INTO :namecnt
		        FROM customer
			WHERE c_w_id = :c_w_id
			AND c_d_id = :c_d_id
		        AND c_last = :c_last;*/
		mysql_stmt = stmt[t_num][13];

    sprintf(para10[0], "%d", c_w_id);
    sprintf(para10[1], "%d", c_d_id);
    sprintf(para10[2], "'%s'", c_last);
    sprintf(executed_sql, "%s", sql14);
    replace_the_para(executed_sql, para10);
    if (mysql_query(mysql, executed_sql)) goto sqlerr;
    result = mysql_store_result(mysql);
    while ((row = mysql_fetch_row(result))) {
      if (row[0]) {
        namecnt = atoi(row[0]);
      }
    }
    mysql_free_result(result);

    /*EXEC_SQL DECLARE c_byname_p CURSOR FOR
      SELECT c_id
      FROM customer
      WHERE c_w_id = :c_w_id 
      AND c_d_id = :c_d_id 
      AND c_last = :c_last
      ORDER BY c_first;

      EXEC_SQL OPEN c_byname_p;*/
    mysql_stmt = stmt[t_num][14];

    sprintf(para10[0], "%d", c_w_id);
    sprintf(para10[1], "%d", c_d_id);
    sprintf(para10[2], "'%s'", c_last);
    sprintf(executed_sql, "%s", sql15);
    replace_the_para(executed_sql, para10);
    if (mysql_query(mysql, executed_sql)) goto sqlerr;
    result = mysql_store_result(mysql);
    while ((row = mysql_fetch_row(result))) {
      if (row[0])
        c_id = atoi(row[0]);
    }
    mysql_free_result(result);

		if (namecnt % 2) 
			namecnt++;	/* Locate midpoint customer; */
	}

	proceed = 6;
	/*EXEC_SQL SELECT c_first, c_middle, c_last, c_street_1,
		        c_street_2, c_city, c_state, c_zip, c_phone,
		        c_credit, c_credit_lim, c_discount, c_balance,
		        c_since
		INTO :c_first, :c_middle, :c_last, :c_street_1,
		     :c_street_2, :c_city, :c_state, :c_zip, :c_phone,
		     :c_credit, :c_credit_lim, :c_discount, :c_balance,
		     :c_since
		FROM customer
	        WHERE c_w_id = :c_w_id 
	        AND c_d_id = :c_d_id 
		AND c_id = :c_id
		FOR UPDATE;*/
	mysql_stmt = stmt[t_num][15];

  sprintf(para10[0], "%d", c_w_id);
  sprintf(para10[1], "%d", c_d_id);
  sprintf(para10[2], "%d", c_id);
  sprintf(executed_sql, "%s", sql16);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;
  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      sprintf(c_first, "%s", row[0]);
    if (row[1])
      sprintf(c_middle, "%s", row[1]);
    if (row[2])
      sprintf(c_last, "%s", row[2]);
    if (row[3])
      sprintf(c_street_1, "%s", row[3]);
    if (row[4])
      sprintf(c_street_2, "%s", row[4]);
    if (row[5])
      sprintf(c_city, "%s", row[5]);
    if (row[6])
      sprintf(c_state, "%s", row[6]);
    if (row[7])
      sprintf(c_zip, "%s", row[7]);
    if (row[8])
      sprintf(c_phone, "%s", row[8]);
    if (row[9])
      sprintf(c_credit, "%s", row[9]);
    if (row[10])
      c_credit_lim = atoi(row[10]);
    if (row[11])
      c_discount = atoi(row[11]);
    if (row[12])
      c_balance = atoi(row[12]);
    if (row[13])
      sprintf(c_since, "%s", row[13]);
  }
  mysql_free_result(result);

	c_balance = c_balance - h_amount;
	c_credit[2] = '\0';
	if (strstr(c_credit, "BC")) {
		proceed = 7;
		/*EXEC_SQL SELECT c_data 
			INTO :c_data
		        FROM customer
		        WHERE c_w_id = :c_w_id 
			AND c_d_id = :c_d_id 
			AND c_id = :c_id; */
		mysql_stmt = stmt[t_num][16];

  sprintf(para10[0], "%d", c_w_id);
  sprintf(para10[1], "%d", c_d_id);
  sprintf(para10[2], "%d", c_id);
  sprintf(executed_sql, "%s", sql17);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;
  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      sprintf(c_data, "%s", row[0]);
  }

  mysql_free_result(result);

		sprintf(c_new_data, 
			"| %4d %2d %4d %2d %4d $%7.2f %12c %24c",
			c_id, c_d_id, c_w_id, d_id,
			w_id, h_amount,
			datetime, c_data);


		strncat(c_new_data, c_data, 
			500 - strlen(c_new_data));

		c_new_data[500] = '\0';

		proceed = 8;
		/*EXEC_SQL UPDATE customer
			SET c_balance = :c_balance, c_data = :c_new_data
			WHERE c_w_id = :c_w_id 
			AND c_d_id = :c_d_id 
			AND c_id = :c_id;*/
    mysql_stmt = stmt[t_num][17];

    sprintf(para10[0], "%f", c_balance);
    sprintf(para10[1], "'%s'", c_data);
    sprintf(para10[2], "%d", c_w_id);
    sprintf(para10[3], "%d", c_d_id);
    sprintf(para10[4], "%d", c_id);
    sprintf(executed_sql, "%s", sql18);
    replace_the_para(executed_sql, para10);
    if (mysql_query(mysql, executed_sql)) goto sqlerr;

  } else {
		proceed = 9;
		/*EXEC_SQL UPDATE customer 
			SET c_balance = :c_balance
			WHERE c_w_id = :c_w_id 
			AND c_d_id = :c_d_id 
			AND c_id = :c_id;*/
		mysql_stmt = stmt[t_num][18];

    sprintf(para10[0], "%f", c_balance);
    sprintf(para10[1], "%d", c_w_id);
    sprintf(para10[2], "%d", c_d_id);
    sprintf(para10[3], "%d", c_id);
    sprintf(executed_sql, "%s", sql19);
    replace_the_para(executed_sql, para10);
    if (mysql_query(mysql, executed_sql)) goto sqlerr;

	}
	strncpy(h_data, w_name, 10);
	h_data[10] = '\0';
	strncat(h_data, d_name, 10);
	h_data[20] = ' ';
	h_data[21] = ' ';
	h_data[22] = ' ';
	h_data[23] = ' ';
	h_data[24] = '\0';

	proceed = 10;
	/*EXEC_SQL INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id,
			                   h_w_id, h_date, h_amount, h_data)
	                VALUES(:c_d_id, :c_w_id, :c_id, :d_id,
		               :w_id, 
			       :datetime,
			       :h_amount, :h_data);*/
	mysql_stmt = stmt[t_num][19];

  sprintf(para10[0], "%d", c_d_id);
  sprintf(para10[1], "%d", c_w_id);
  sprintf(para10[2], "%d", c_id);
  sprintf(para10[3], "%d", d_id);
  sprintf(para10[4], "%d", w_id);
  sprintf(para10[5], "'%s'", datetime);
  sprintf(para10[6], "%f", h_amount);
  sprintf(para10[7], "%f", h_data);
  sprintf(executed_sql, "%s", sql20);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

	/*EXEC_SQL COMMIT WORK;*/
	if( mysql_commit(ctx[t_num]) ) goto sqlerr;

	return (1);

sqlerr:
        fprintf(stderr, "payment %d:%d\n",t_num,proceed);
	error(ctx[t_num],mysql_stmt);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);
sqlerrerr:
	return (0);
}
