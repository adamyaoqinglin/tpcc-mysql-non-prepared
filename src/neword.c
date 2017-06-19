/*
 * -*-C-*- 
 * neword.pc 
 * corresponds to A.1 in appendix A
 */

#include <stdio.h>
#include <string.h>
#include <time.h>

#include <mysql.h>

#include "spt_proc.h"
#include "tpc.h"

#define pick_dist_info(ol_dist_info,ol_supply_w_id) \
switch(ol_supply_w_id) { \
case 1: strncpy(ol_dist_info, s_dist_01, 25); break; \
case 2: strncpy(ol_dist_info, s_dist_02, 25); break; \
case 3: strncpy(ol_dist_info, s_dist_03, 25); break; \
case 4: strncpy(ol_dist_info, s_dist_04, 25); break; \
case 5: strncpy(ol_dist_info, s_dist_05, 25); break; \
case 6: strncpy(ol_dist_info, s_dist_06, 25); break; \
case 7: strncpy(ol_dist_info, s_dist_07, 25); break; \
case 8: strncpy(ol_dist_info, s_dist_08, 25); break; \
case 9: strncpy(ol_dist_info, s_dist_09, 25); break; \
case 10: strncpy(ol_dist_info, s_dist_10, 25); break; \
}

extern MYSQL **ctx;
extern MYSQL_STMT ***stmt;

extern FILE *ftrx_file;

#define NNULL ((void *)0)

const char* sql1 = "SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?";
const char* sql2 = "SELECT d_next_o_id, d_tax FROM district WHERE d_id = ? AND d_w_id = ? FOR UPDATE";
const char* sql3 = "UPDATE district SET d_next_o_id = ? + 1 WHERE d_id = ? AND d_w_id = ?";
const char* sql4 = "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES(?, ?, ?, ?, ?, ?, ?)";
const char* sql5 = "INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES (?,?,?)";
const char* sql6 = "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?";
const char* sql7 = "SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_i_id = ? AND s_w_id = ? FOR UPDATE";
const char* sql8 = "UPDATE stock SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?";
const char* sql9 = "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
/*
 * the new order transaction
 */
int neword( int t_num,
	    int w_id_arg,		/* warehouse id */
	    int d_id_arg,		/* district id */
	    int c_id_arg,		/* customer id */
	    int o_ol_cnt_arg,	        /* number of items */
	    int o_all_local_arg,	/* are all order lines local */
	    int itemid[],		/* ids of items to be ordered */
	    int supware[],		/* warehouses supplying items */
	    int qty[]		        /* quantity of each item */
)
{

	int            w_id = w_id_arg;
	int            d_id = d_id_arg;
	int            c_id = c_id_arg;
	int            o_ol_cnt = o_ol_cnt_arg;
	int            o_all_local = o_all_local_arg;
	float           c_discount;
	char            c_last[17];
	char            c_credit[3];
	float           w_tax;
	int            d_next_o_id;
	float           d_tax;
	char            datetime[81];
	int            o_id;
	char            i_name[25];
	float           i_price;
	char            i_data[51];
	int            ol_i_id;
	int            s_quantity;
	char            s_data[51];
	char            s_dist_01[25];
	char            s_dist_02[25];
	char            s_dist_03[25];
	char            s_dist_04[25];
	char            s_dist_05[25];
	char            s_dist_06[25];
	char            s_dist_07[25];
	char            s_dist_08[25];
	char            s_dist_09[25];
	char            s_dist_10[25];
	char            ol_dist_info[25];
	int            ol_supply_w_id;
	float           ol_amount;
	int            ol_number;
	int            ol_quantity;

	char            iname[MAX_NUM_ITEMS][MAX_ITEM_LEN];
	char            bg[MAX_NUM_ITEMS];
	float           amt[MAX_NUM_ITEMS];
	float           price[MAX_NUM_ITEMS];
	int            stock[MAX_NUM_ITEMS];
	float           total = 0.0;

	int            min_num;
	int            i,j,tmp,swp;
	int            ol_num_seq[MAX_NUM_ITEMS];

	int             proceed = 0;
 	struct timespec tbuf1,tbuf_start;
	clock_t clk1,clk_start;	

  MYSQL * mysql = ctx[t_num];
	MYSQL_STMT*   mysql_stmt;
        MYSQL_BIND    param[9];
	MYSQL_BIND    column[12];
  MYSQL_RES* result;
  MYSQL_ROW row;

	/* EXEC SQL WHENEVER NOT FOUND GOTO sqlerr;*/
	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	/*EXEC SQL CONTEXT USE :ctx[t_num];*/

        gettimestamp(datetime, STRFTIME_FORMAT, TIMESTAMP_LEN);
	clk_start = clock_gettime(CLOCK_REALTIME, &tbuf_start );

	proceed = 1;
	/*EXEC_SQL SELECT c_discount, c_last, c_credit, w_tax
		INTO :c_discount, :c_last, :c_credit, :w_tax
	        FROM customer, warehouse
	        WHERE w_id = :w_id 
		AND c_w_id = w_id 
		AND c_d_id = :d_id 
		AND c_id = :c_id;*/
	mysql_stmt = stmt[t_num][0];

  char para10[10][10000];
  sprintf(para10[0], "%d", w_id);
  sprintf(para10[1], "%d", d_id);
  sprintf(para10[2], "%d", c_id);
  char executed_sql[50000];
  sprintf(executed_sql, "%s", sql1);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      c_discount = strtod(row[0], NULL);
    if (row[1])
      sprintf(c_last, "%s", row[1]);
    if (row[2])
      sprintf(c_credit, "%s", row[2]);
    if (row[3])
      w_tax = strtod(row[3], NULL);
  }
  mysql_free_result(result);

#ifdef DEBUG
	printf("n %d\n",proceed);
#endif

	proceed = 2;
	/*EXEC_SQL SELECT d_next_o_id, d_tax INTO :d_next_o_id, :d_tax
	        FROM district
	        WHERE d_id = :d_id
		AND d_w_id = :w_id
		FOR UPDATE;*/
	mysql_stmt = stmt[t_num][1];

  sprintf(para10[0], "%d", d_id);
  sprintf(para10[1], "%d", w_id);
  sprintf(executed_sql, "%s", sql2);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

  result = mysql_store_result(mysql);
  while ((row = mysql_fetch_row(result))) {
    if (row[0])
      d_next_o_id = atoi(row[0]);
    if (row[1])
      d_tax = strtod(row[1], NULL);
  }
  mysql_free_result(result);

	proceed = 3;
	/*EXEC_SQL UPDATE district SET d_next_o_id = :d_next_o_id + 1
	        WHERE d_id = :d_id 
		AND d_w_id = :w_id;*/
	mysql_stmt = stmt[t_num][2];

  sprintf(para10[0], "%d", d_next_o_id);
  sprintf(para10[1], "%d", d_id);
  sprintf(para10[2], "%d", w_id);
  sprintf(executed_sql, "%s", sql3);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

	o_id = d_next_o_id;

#ifdef DEBUG
	printf("n %d\n",proceed);
#endif

	proceed = 4;
	/*EXEC_SQL INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,
			             o_entry_d, o_ol_cnt, o_all_local)
		VALUES(:o_id, :d_id, :w_id, :c_id, 
		       :datetime,
                       :o_ol_cnt, :o_all_local);*/
	mysql_stmt = stmt[t_num][3];

  sprintf(para10[0], "%d", o_id);
  sprintf(para10[1], "%d", d_id);
  sprintf(para10[2], "%d", w_id);
  sprintf(para10[3], "%d", c_id);
  sprintf(para10[4], "'%s'", datetime);
  sprintf(para10[5], "%d", o_ol_cnt);
  sprintf(para10[6], "%d", o_all_local);
  sprintf(executed_sql, "%s", sql4);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

#ifdef DEBUG
	printf("n %d\n",proceed);
#endif

	proceed = 5;
	/* EXEC_SQL INSERT INTO new_orders (no_o_id, no_d_id, no_w_id)
	   VALUES (:o_id,:d_id,:w_id); */
	mysql_stmt = stmt[t_num][4];

  sprintf(para10[0], "%d", o_id);
  sprintf(para10[1], "%d", d_id);
  sprintf(para10[2], "%d", w_id);
  sprintf(executed_sql, "%s", sql5);
  replace_the_para(executed_sql, para10);
  if (mysql_query(mysql, executed_sql)) goto sqlerr;

	/* sort orders to avoid DeadLock */
	for (i = 0; i < o_ol_cnt; i++) {
		ol_num_seq[i]=i;
	}
	for (i = 0; i < (o_ol_cnt - 1); i++) {
		tmp = (MAXITEMS + 1) * supware[ol_num_seq[i]] + itemid[ol_num_seq[i]];
		min_num = i;
		for ( j = i+1; j < o_ol_cnt; j++) {
		  if ( (MAXITEMS + 1) * supware[ol_num_seq[j]] + itemid[ol_num_seq[j]] < tmp ){
		    tmp = (MAXITEMS + 1) * supware[ol_num_seq[j]] + itemid[ol_num_seq[j]];
		    min_num = j;
		  }
		}
		if ( min_num != i ){
		  swp = ol_num_seq[min_num];
		  ol_num_seq[min_num] = ol_num_seq[i];
		  ol_num_seq[i] = swp;
		}
	}


	for (ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
		ol_supply_w_id = supware[ol_num_seq[ol_number - 1]];
		ol_i_id = itemid[ol_num_seq[ol_number - 1]];
		ol_quantity = qty[ol_num_seq[ol_number - 1]];

		/* EXEC SQL WHENEVER NOT FOUND GOTO invaliditem; */
		proceed = 6;
		/*EXEC_SQL SELECT i_price, i_name, i_data
			INTO :i_price, :i_name, :i_data
		        FROM item
		        WHERE i_id = :ol_i_id;*/
		mysql_stmt = stmt[t_num][5];

    sprintf(para10[0], "%d", ol_i_id);
    sprintf(executed_sql, "%s", sql6);
    replace_the_para(executed_sql, para10);
    if (mysql_query(mysql, executed_sql)) goto sqlerr;

    result = mysql_store_result(mysql);
    while ((row = mysql_fetch_row(result))) {
      if (row[0])
        i_price = strtod(row[0], NULL);
      if (row[1])
        sprintf(i_name, "%s", row[1]);
      if (row[2])
        sprintf(i_data, "%s", row[2]);
    }
    mysql_free_result(result);

		price[ol_num_seq[ol_number - 1]] = i_price;
		strncpy(iname[ol_num_seq[ol_number - 1]], i_name, 25);

		/* EXEC SQL WHENEVER NOT FOUND GOTO sqlerr; */

#ifdef DEBUG
		printf("n %d\n",proceed);
#endif

		proceed = 7;
		/*EXEC_SQL SELECT s_quantity, s_data, s_dist_01, s_dist_02,
		                s_dist_03, s_dist_04, s_dist_05, s_dist_06,
		                s_dist_07, s_dist_08, s_dist_09, s_dist_10
			INTO :s_quantity, :s_data, :s_dist_01, :s_dist_02,
		             :s_dist_03, :s_dist_04, :s_dist_05, :s_dist_06,
		             :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
		        FROM stock
		        WHERE s_i_id = :ol_i_id 
			AND s_w_id = :ol_supply_w_id
			FOR UPDATE;*/
		mysql_stmt = stmt[t_num][6];

    sprintf(para10[0], "%d", ol_i_id);
    sprintf(para10[1], "%d", ol_supply_w_id);
    sprintf(executed_sql, "%s", sql7);
    replace_the_para(executed_sql, para10);
    if (mysql_query(mysql, executed_sql)) goto sqlerr;

    result = mysql_store_result(mysql);
    while ((row = mysql_fetch_row(result))) {
      if (row[0])
        s_quantity = atoi(row[0]);
      if (row[1])
        sprintf(s_data, "%s", row[1]);
      if (row[2])
        sprintf(s_dist_01, "%s", row[2]);
      if (row[3])
        sprintf(s_dist_02, "%s", row[3]);
      if (row[4])
        sprintf(s_dist_03, "%s", row[4]);
      if (row[5])
        sprintf(s_dist_04, "%s", row[5]);
      if (row[6])
        sprintf(s_dist_05, "%s", row[6]);
      if (row[7])
        sprintf(s_dist_06, "%s", row[7]);
      if (row[8])
        sprintf(s_dist_07, "%s", row[8]);
      if (row[9])
        sprintf(s_dist_08, "%s", row[9]);
      if (row[10])
        sprintf(s_dist_09, "%s", row[10]);
      if (row[11])
        sprintf(s_dist_10, "%s", row[11]);
    }
    mysql_free_result(result);

		pick_dist_info(ol_dist_info, d_id);	/* pick correct
							 * s_dist_xx */

		stock[ol_num_seq[ol_number - 1]] = s_quantity;

		if ((strstr(i_data, "original") != NULL) &&
		    (strstr(s_data, "original") != NULL))
			bg[ol_num_seq[ol_number - 1]] = 'B';
		else
			bg[ol_num_seq[ol_number - 1]] = 'G';

		if (s_quantity > ol_quantity)
			s_quantity = s_quantity - ol_quantity;
		else
			s_quantity = s_quantity - ol_quantity + 91;

#ifdef DEBUG
		printf("n %d\n",proceed);
#endif

		proceed = 8;
		/*EXEC_SQL UPDATE stock SET s_quantity = :s_quantity
		        WHERE s_i_id = :ol_i_id 
			AND s_w_id = :ol_supply_w_id;*/
		mysql_stmt = stmt[t_num][7];

    sprintf(para10[0], "%d", s_quantity);
    sprintf(para10[1], "%d", ol_i_id);
    sprintf(para10[2], "%d", ol_supply_w_id);
    sprintf(executed_sql, "%s", sql8);
    replace_the_para(executed_sql, para10);
    if (mysql_query(mysql, executed_sql)) goto sqlerr;

		ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);
		amt[ol_num_seq[ol_number - 1]] = ol_amount;
		total += ol_amount;

#ifdef DEBUG
		printf("n %d\n",proceed);
#endif

		proceed = 9;
		/*EXEC_SQL INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, 
						 ol_number, ol_i_id, 
						 ol_supply_w_id, ol_quantity, 
						 ol_amount, ol_dist_info)
			VALUES (:o_id, :d_id, :w_id, :ol_number, :ol_i_id,
				:ol_supply_w_id, :ol_quantity, :ol_amount,
				:ol_dist_info);*/
		mysql_stmt = stmt[t_num][8];

    sprintf(para10[0], "%d", o_id);
    sprintf(para10[1], "%d", d_id);
    sprintf(para10[2], "%d", w_id);
    sprintf(para10[3], "%d", ol_number);
    sprintf(para10[4], "%d", ol_i_id);
    sprintf(para10[5], "%d", ol_supply_w_id);
    sprintf(para10[6], "%d", ol_quantity);
    sprintf(para10[7], "%f", ol_amount);
    sprintf(para10[8], "'%s'", ol_dist_info);
    sprintf(executed_sql, "%s", sql9);
    replace_the_para(executed_sql, para10);
    if (mysql_query(mysql, executed_sql)) goto sqlerr;

	}			/* End Order Lines */

#ifdef DEBUG
	printf("insert 3\n");
	fflush(stdout);
#endif

	/*EXEC_SQL COMMIT WORK;*/
	if( mysql_commit(ctx[t_num]) ) goto sqlerr;
	clk1 = clock_gettime(CLOCK_REALTIME, &tbuf1 );
	if (ftrx_file) {
		fprintf(ftrx_file,"t_num: %d finish: %lu %lu start: %lu %lu\n",t_num, tbuf1.tv_sec, tbuf1.tv_nsec,
			tbuf_start.tv_sec, tbuf_start.tv_nsec);
	}

	return (1);

invaliditem:
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);

	/* printf("Item number is not valid\n"); */
	return (1); /* OK? */

sqlerr:
	fprintf(stderr,"neword %d:%d\n",t_num,proceed);
      	error(ctx[t_num],mysql_stmt);
	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);
sqlerrerr:
	return (0);
}

