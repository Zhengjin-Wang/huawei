# useage: 
# step1: ./hyriseServer -p 5433 --benchmark_data ch_bench:1
# step2: python ch_bench.py

import psycopg2
import random
import time
import threading
import argparse
import queue
import json
from datetime import datetime
import os
os.environ['PGGSSENCMODE'] = 'disable'

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("-n", "--num_warehouse", type=int, help="Number of warehouses", default=1)
parser.add_argument("-cc", "--num_tpcc_threads", type=int, help="Number of TPCC threads", default=1)
parser.add_argument("-ch", "--num_tpch_threads", type=int, help="Number of TPCH threads", default=1)
parser.add_argument("-t", "--run_time", type=int, help="Run time", default=300)
parser.add_argument("-p", "--port", type=str, help="Server port", default='5435')
args = parser.parse_args()

# Get the values from command line arguments
num_warehouse = args.num_warehouse
num_tpcc_threads = args.num_tpcc_threads
num_tpch_threads = args.num_tpch_threads
run_time = args.run_time
port = args.port

seed = 2024
random.seed(seed)

# 打印数据函数
def show_data(str, data):
    arr = []
    for item in data:
        arr.append(item)
    print(str, arr)

class TPCC:
    def __init__(self, num_warehouse, port, time, id, queue):
        self.num_warehouse = num_warehouse
        self.db = psycopg2.connect(host='localhost', port=port)
        self.cursor = self.db.cursor()
        self.exec_count = {'delivery': 0, 'new_order': 0, 'order_status': 0, 'payment': 0, 'stock_level': 0}
        self.exec_success_count = {'delivery': 0, 'new_order': 0, 'order_status': 0, 'payment': 0, 'stock_level': 0}
        self.exec_failed_count = {'delivery': 0, 'new_order': 0, 'order_status': 0, 'payment': 0, 'stock_level': 0}
        self.exec_weight = {'delivery': 4, 'new_order': 45, 'order_status': 4, 'payment': 43, 'stock_level': 4}
        self.run_time = time
        self.exec_time = {'delivery': 0, 'new_order': 0, 'order_status': 0, 'payment': 0, 'stock_level': 0}
        self.id = id
        self.queue = queue

    def __del__(self):
        self.cursor.close()
        self.db.close()

    def run(self):
        start_time = time.time()
        while(time.time() - start_time < self.run_time):
            try:
                transaction = random.choices(list(self.exec_weight.keys()), weights=list(self.exec_weight.values()))[0]
                trans_start_time = time.time()
                if transaction == 'delivery':
                    self.delivery_trans()
                elif transaction == 'new_order':
                    self.new_order_trans()
                elif transaction == 'order_status':
                    self.order_status_trans()
                elif transaction == 'payment':
                    self.payment_trans()
                elif transaction == 'stock_level':
                    self.stock_level_trans()
                trans_end_time = time.time()
                self.exec_time[transaction] += trans_end_time - trans_start_time
                self.exec_success_count[transaction] += 1
            except Exception as e:
                # 处理其他异常
                # print(e)
                self.exec_failed_count[transaction] += 1
            self.exec_count[transaction] += 1
        result = {
            "id": self.id,
            "type": "tpcc",
            "success_count": self.exec_success_count,
            "failed_count": self.exec_failed_count,
            "exec_count": self.exec_count,
            "exec_time": self.exec_time
        }
        self.queue.put(result)
        print('tpcc执行成功次数:', self.exec_success_count)
        print('tpcc执行失败次数:', self.exec_failed_count)
        print('tpcc执行次数:', self.exec_count)
        print('tpcc执行耗时(s):', self.exec_time)

    def delivery_trans(self):
        w_id = random.randint(1, num_warehouse)
        o_carrier_id = random.randint(1, 10)
        ol_delivery_d = int(time.time())
        for d_id in range(1, 10):
            self.cursor.execute(f"SELECT MIN(NO_O_ID) FROM NEW_ORDER WHERE NO_W_ID = {w_id}  AND NO_D_ID = {d_id}" )
            # min_no_o_id = self.cursor.fetchall()[0][0]
            min_no_o_id = self.cursor.fetchone()[0]
            no_o_id = min_no_o_id
            self.cursor.execute(f"DELETE FROM NEW_ORDER WHERE NO_W_ID = {w_id} AND NO_D_ID = {d_id} AND NO_O_ID = {no_o_id}" )
            self.cursor.execute(f"SELECT O_C_ID FROM \"ORDER\" WHERE O_W_ID = {w_id} AND O_D_ID = {d_id} AND O_ID = {no_o_id}" )
            o_c_id = self.cursor.fetchone()[0]
            self.cursor.execute(f"UPDATE \"ORDER\" SET O_CARRIER_ID = {o_carrier_id} WHERE O_W_ID = {w_id} AND O_D_ID = {d_id} AND O_ID = {no_o_id}" )
            self.cursor.execute(f"SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_W_ID = {w_id} AND OL_D_ID = {d_id} AND OL_O_ID = {no_o_id}" )
            amount = self.cursor.fetchone()[0]
            self.cursor.execute(f"UPDATE ORDER_LINE SET OL_DELIVERY_D = {ol_delivery_d} WHERE OL_W_ID = {w_id} AND OL_D_ID = {d_id} AND OL_O_ID = {no_o_id}" )
            self.cursor.execute(f"UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + {amount}, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE C_W_ID = {w_id} AND C_D_ID = {d_id} AND C_ID = {o_c_id}" )
        self.db.commit()
    
    def new_order_trans(self):
        w_id = random.randint(1, num_warehouse)
        d_id = random.randint(1, 10)
        c_id = random.randint(1, 3000)
        ol_cnt = random.randint(5, 15)
        all_local = 1

        # Get the customer's information
        self.cursor.execute(f"SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = {w_id} AND C_D_ID = {d_id} AND C_ID = {c_id}")
        c_discount, c_last, c_credit = self.cursor.fetchone()

        # Get the district's information
        self.cursor.execute(f"SELECT D_TAX FROM DISTRICT WHERE D_W_ID = {w_id} AND D_ID = {d_id}")
        d_tax = self.cursor.fetchone()[0]

        # Get the warehouse's information
        self.cursor.execute(f"SELECT W_TAX FROM WAREHOUSE WHERE W_ID = {w_id}")
        w_tax = self.cursor.fetchone()[0]

        # Generate the new order identifier
        self.cursor.execute(f"SELECT D_NEXT_O_ID, D_NEXT_O_ID - 1 FROM DISTRICT WHERE D_W_ID = {w_id} AND D_ID = {d_id}")
        d_next_o_id, o_id = self.cursor.fetchone()
        self.cursor.execute(f"UPDATE DISTRICT SET D_NEXT_O_ID = {d_next_o_id + 1} WHERE D_W_ID = {w_id} AND D_ID = {d_id}")

        # Insert the new order
        self.cursor.execute(f"INSERT INTO \"ORDER\" (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES ({o_id}, {d_id}, {w_id}, {c_id}, {int(time.time())}, NULL, {ol_cnt}, {all_local})")

        # Insert the new order lines
        total_amount = 0
        for i in range(ol_cnt):
            ol_number = i + 1
            ol_i_id = random.randint(1, 100000)
            ol_supply_w_id = w_id
            ol_quantity = 5
            ol_amount = 0

            # Get the item's information
            self.cursor.execute(f"SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = {ol_i_id}")
            i_price, i_name, i_data = self.cursor.fetchone()

            # Get the stock's information
            self.cursor.execute(f"SELECT S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA FROM STOCK WHERE S_I_ID = {ol_i_id} AND S_W_ID = {ol_supply_w_id}")
            s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data = self.cursor.fetchone()

            # Update the stock
            if s_quantity - ol_quantity >= 10:
                s_quantity -= ol_quantity
            else:
                s_quantity += 91 - ol_quantity
            s_ytd += ol_quantity
            s_order_cnt += 1
            if ol_supply_w_id != w_id:
                s_remote_cnt += 1
            self.cursor.execute(f"UPDATE STOCK SET S_QUANTITY = {s_quantity}, S_YTD = {s_ytd}, S_ORDER_CNT = {s_order_cnt}, S_REMOTE_CNT = {s_remote_cnt} WHERE S_I_ID = {ol_i_id} AND S_W_ID = {ol_supply_w_id}")

            # Calculate the order line amount
            ol_amount = ol_quantity * i_price

            # Insert the order line
            self.cursor.execute(f"INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES ({o_id}, {d_id}, {w_id}, {ol_number}, {ol_i_id}, {ol_supply_w_id}, {ol_quantity}, {ol_amount}, 'info')")

            # Update the total amount
            total_amount += ol_amount

        # Calculate the total amount
        total_amount *= (1 - c_discount) * (1 + w_tax + d_tax)

        # Update the customer's balance and delivery count
        self.cursor.execute(f"UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + {total_amount}, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE C_W_ID = {w_id} AND C_D_ID = {d_id} AND C_ID = {c_id}")

        self.db.commit()

    def order_status_trans(self):
        w_id = random.randint(1, num_warehouse)
        d_id = random.randint(1, 10)
        c_id = random.randint(1, 3000)
        self.cursor.execute(f"SELECT COUNT(*) FROM \"ORDER\" WHERE O_W_ID = {w_id} AND O_D_ID = {d_id} AND O_C_ID = {c_id}")
        order_count = self.cursor.fetchone()[0]
        self.cursor.execute(f"SELECT O_ID, O_ENTRY_D, O_CARRIER_ID FROM \"ORDER\" WHERE O_W_ID = {w_id} AND O_D_ID = {d_id} AND O_C_ID = {c_id} ORDER BY O_ID DESC LIMIT 1")
        o_id, o_entry_d, o_carrier_id = self.cursor.fetchone()
        self.cursor.execute(f"SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = {w_id} AND OL_D_ID = {d_id} AND OL_O_ID = {o_id}")
        order_lines = self.cursor.fetchall()
        self.cursor.execute(f"SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = {w_id} AND C_D_ID = {d_id} AND C_ID = {c_id}")
        c_first, c_middle, c_last, c_balance = self.cursor.fetchone()
        self.db.commit()
        # return order_count, o_id, o_entry_d, o_carrier_id, order_lines, c_first, c_middle, c_last, c_balance
    
    def payment_trans(self):
        w_id = random.randint(1, num_warehouse)
        d_id = random.randint(1, 10)
        c_w_id = random.randint(1, num_warehouse)
        c_d_id = random.randint(1, 10)
        c_id = random.randint(1, 3000)
        h_amount = random.uniform(1, 5000)
        h_date = int(time.time())
        self.cursor.execute(f"SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, C_CREDIT FROM CUSTOMER WHERE C_W_ID = {c_w_id} AND C_D_ID = {c_d_id} AND C_ID = {c_id}")
        c_first, c_middle, c_last, c_balance, c_credit = self.cursor.fetchone()
        self.cursor.execute(f"UPDATE CUSTOMER SET C_BALANCE = C_BALANCE - {h_amount}, C_YTD_PAYMENT = C_YTD_PAYMENT + {h_amount}, C_PAYMENT_CNT = C_PAYMENT_CNT + 1 WHERE C_W_ID = {c_w_id} AND C_D_ID = {c_d_id} AND C_ID = {c_id}")
        self.cursor.execute(f"SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME FROM WAREHOUSE WHERE W_ID = {w_id}")
        w_street_1, w_street_2, w_city, w_state, w_zip, w_name = self.cursor.fetchone()
        self.cursor.execute(f"SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME FROM DISTRICT WHERE D_W_ID = {w_id} AND D_ID = {d_id}")
        d_street_1, d_street_2, d_city, d_state, d_zip, d_name = self.cursor.fetchone()
        self.cursor.execute(f"INSERT INTO HISTORY (H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) VALUES ({c_id}, {c_d_id}, {c_w_id}, {d_id}, {w_id}, {h_date}, {h_amount}, 'data')")
        self.db.commit()

    def stock_level_trans(self):
        w_id = random.randint(1, num_warehouse)
        d_id = random.randint(1, 10)
        threshold = random.randint(10, 20)
        self.cursor.execute(f"SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = {w_id} AND D_ID = {d_id}")
        d_next_o_id = self.cursor.fetchone()[0]
        self.cursor.execute(f"SELECT COUNT(DISTINCT S_I_ID) FROM ORDER_LINE, STOCK WHERE OL_W_ID = {w_id} AND OL_D_ID = {d_id} AND OL_O_ID < {d_next_o_id} AND OL_O_ID >= {d_next_o_id - 20} AND S_W_ID = {w_id} AND S_I_ID = OL_I_ID AND S_QUANTITY < {threshold}")
        low_stock_count = self.cursor.fetchone()[0]
        self.db.commit()

class TPCH():
    def __init__(self, num_warehouse, port, time, queue, id):
        self.num_warehouse = num_warehouse
        self.db = psycopg2.connect(host='localhost', port=port)
        self.cursor = self.db.cursor()
        self.exec_success_count = [0]*22
        self.exec_failed_count = [0]*22
        self.exec_count = [0]*22
        self.exec_time = [0]*22
        self.run_time = time
        self.queue = queue
        self.id = id

    def run(self):
        start_time = time.time()
        exec_query = self.id % 22 + 1
        while(time.time() - start_time < self.run_time):
            query_start_time = time.time()
            try:
                if exec_query == 1:
                    self.q1()
                elif exec_query == 2:
                    self.q2()
                elif exec_query == 3:
                    self.q3()
                elif exec_query == 4:
                    self.q4()
                elif exec_query == 5:
                    self.q5()
                elif exec_query == 6:
                    self.q6()
                elif exec_query == 7:
                    self.q7()
                elif exec_query == 8:
                    self.q8()
                elif exec_query == 9:
                    self.q9()
                elif exec_query == 10:
                    self.q10()
                elif exec_query == 11:
                    self.q11()
                elif exec_query == 12:
                    self.q12()
                elif exec_query == 13:
                    self.q13()
                elif exec_query == 14:
                    self.q14()
                elif exec_query == 15:
                    self.q15()
                elif exec_query == 16:
                    self.q16()
                elif exec_query == 17:
                    self.q17()
                elif exec_query == 18:
                    self.q18()
                elif exec_query == 19:
                    self.q19()
                elif exec_query == 20:
                    self.q20()
                elif exec_query == 21:
                    self.q21()
                elif exec_query == 22:
                    self.q22()
                query_end_time = time.time()
                self.exec_time[exec_query-1] += query_end_time - query_start_time
                self.exec_success_count[exec_query-1] += 1
            except Exception as e:
                # 处理其他异常
                # print(e)
                self.exec_failed_count[exec_query-1] += 1
            self.exec_count[exec_query-1] += 1
            exec_query += 1
            if (exec_query >= 23):
                exec_query = 1
        result = {
            "id": self.id,
            "type": "tpch",
            "success_count": self.exec_success_count,
            "failed_count": self.exec_failed_count,
            "exec_count": self.exec_count,
            "exec_time": self.exec_time
        }
        self.queue.put(result)
        print('tpch执行成功次数:', self.exec_success_count)
        print('tpch执行失败次数:', self.exec_failed_count)
        print('tpch执行次数:', self.exec_count)
        print('tpch执行耗时(s):', self.exec_time)

    def q1(self):
        self.cursor.execute('''SELECT   OL_NUMBER,
SUM(OL_QUANTITY) AS SUM_QTY,
SUM(OL_AMOUNT) AS SUM_AMOUNT,
AVG(OL_QUANTITY) AS AVG_QTY,
AVG(OL_AMOUNT) AS AVG_AMOUNT,
COUNT(*) AS COUNT_ORDER
FROM	 ORDER_LINE
GROUP BY OL_NUMBER ORDER BY OL_NUMBER;''')


#     def q2(self):
#         self.cursor.execute('''select s_suppkey, s_name, n_name, I_ID, I_NAME, s_address, s_phone, s_comment
# from     ITEM, supplier, STOCK as b, nation, region,
#      (select a.S_I_ID as m_i_id,
#          min(a.S_QUANTITY) as m_s_quantity
#      from     STOCK as a, supplier, nation, region
#      where     ((a.S_W_ID*a.S_I_ID)%10000)=s_suppkey
#           and s_nationkey=n_nationkey
#           and n_regionkey=r_regionkey
#           and r_name like 'EUROP%'
#      group by a.S_I_ID) m
# where      I_ID = b.S_I_ID
#      and ((b.S_W_ID * b.S_I_ID)%10000) = s_suppkey
#      and s_nationkey = n_nationkey
#      and n_regionkey = r_regionkey
#      and I_DATA like '%b'
#      and r_name like 'EUROP%'
#      and I_ID=m_i_id
#      and b.S_QUANTITY = m_s_quantity
# order by n_name, s_name, I_ID LIMIT 10;''')

    def q2(self):
        self.cursor.execute('''select s_suppkey, s_name, n_name, I_ID, I_NAME, s_address, s_phone, s_comment
from     ITEM, supplier, STOCK as b, nation, region,
     (select a.S_I_ID as m_i_id,
         min(a.S_QUANTITY) as m_s_quantity
     from     STOCK as a, supplier, nation, region
     where     a.S_I_ID =s_suppkey
          and s_nationkey=n_nationkey
          and n_regionkey=r_regionkey
          and r_name like 'EUROP%'
     group by a.S_I_ID) m
where      I_ID = b.S_I_ID
     and b.S_I_ID = s_suppkey
     and s_nationkey = n_nationkey
     and n_regionkey = r_regionkey
     and I_DATA like '%b'
     and r_name like 'EUROP%'
     and I_ID=m_i_id
     and b.S_QUANTITY = m_s_quantity
order by n_name, s_name, I_ID LIMIT 10;''')

    def q3(self):
        self.cursor.execute('''SELECT   OL_O_ID, OL_W_ID, OL_D_ID,
SUM(OL_AMOUNT) AS REVENUE, O_ENTRY_D
FROM      CUSTOMER, NEW_ORDER, "ORDER", ORDER_LINE
WHERE      C_STATE LIKE 'a%'
AND C_ID = O_C_ID
AND C_W_ID = O_W_ID
AND C_D_ID = O_D_ID
AND NO_W_ID = O_W_ID
AND NO_D_ID = O_D_ID
AND NO_O_ID = O_ID
AND OL_W_ID = O_W_ID
AND OL_D_ID = O_D_ID
AND OL_O_ID = O_ID
GROUP BY OL_O_ID, OL_W_ID, OL_D_ID, O_ENTRY_D
ORDER BY REVENUE DESC, O_ENTRY_D LIMIT 10;''')

#     def q4(self):
#         self.cursor.execute('''SELECT O_OL_CNT, COUNT(*) AS ORDER_COUNT
# FROM "ORDER"
#     WHERE EXISTS (SELECT *
#             FROM ORDER_LINE
#             WHERE O_ID = OL_O_ID
#             AND O_W_ID = OL_W_ID
#             AND O_D_ID = OL_D_ID
#             AND OL_DELIVERY_D >= O_ENTRY_D)
# GROUP    BY O_OL_CNT
# ORDER    BY O_OL_CNT LIMIT 10;''')

    def q4(self):
        self.cursor.execute('''SELECT O.O_OL_CNT, COUNT(*) AS ORDER_COUNT
FROM "ORDER" O
JOIN ORDER_LINE OL ON O.O_ID = OL.OL_O_ID
                   AND O.O_W_ID = OL.OL_W_ID
                   AND O.O_D_ID = OL.OL_D_ID
WHERE OL.OL_DELIVERY_D >= O.O_ENTRY_D
GROUP BY O.O_OL_CNT
ORDER BY O.O_OL_CNT
LIMIT 10;''')

#     def q5(self):
#         self.cursor.execute('''SELECT n_name,
# SUM(OL_AMOUNT) AS REVENUE
# FROM     CUSTOMER, "ORDER", ORDER_LINE, STOCK, supplier, nation, region
# WHERE     C_ID = O_C_ID
# AND C_W_ID = O_W_ID
# AND C_D_ID = O_D_ID
# AND OL_O_ID = O_ID
# AND OL_W_ID = O_W_ID
# AND OL_D_ID=O_D_ID
# AND OL_W_ID = S_W_ID
# AND OL_I_ID = S_I_ID
# AND ((S_W_ID * S_I_ID)%10000) = s_suppkey
# AND s_nationkey = n_nationkey
# AND n_regionkey = r_regionkey
# AND r_name = 'EUROPE'
# AND (C_ID%25) = s_nationkey
# GROUP BY n_name
# ORDER BY REVENUE DESC LIMIT 10;''')

    def q5(self):
        self.cursor.execute('''SELECT n_name,
SUM(OL_AMOUNT) AS REVENUE
FROM     CUSTOMER, "ORDER", ORDER_LINE, STOCK, supplier, nation, region
WHERE     C_ID = O_C_ID
AND C_W_ID = O_W_ID
AND C_D_ID = O_D_ID
AND OL_O_ID = O_ID
AND OL_W_ID = O_W_ID
AND OL_D_ID=O_D_ID
AND OL_W_ID = S_W_ID
AND OL_I_ID = S_I_ID
AND S_I_ID = s_suppkey
AND s_nationkey = n_nationkey
AND n_regionkey = r_regionkey
AND r_name = 'EUROPE'
AND (C_ID%25) = s_nationkey
GROUP BY n_name
ORDER BY REVENUE DESC LIMIT 10;''')

    def q6(self):
        self.cursor.execute('''SELECT    SUM(OL_AMOUNT) AS REVENUE
FROM ORDER_LINE
WHERE OL_QUANTITY BETWEEN 1 AND 100000 LIMIT 10''')

    def q7(self):
        self.cursor.execute('''SELECT     s_nationkey AS SUPP_NATION,
SUBSTR(C_STATE,1,1) AS CUST_NATION,
((O_ENTRY_D/31536000)+1970) AS L_YEAR,
SUM(OL_AMOUNT) AS REVENUE
FROM     supplier, STOCK, ORDER_LINE, "ORDER", CUSTOMER, nation N1, nation N2
WHERE     OL_SUPPLY_W_ID = S_W_ID
AND OL_I_ID = S_I_ID
AND ((S_W_ID * S_I_ID)%10000) = s_suppkey
AND OL_W_ID = O_W_ID
AND OL_D_ID = O_D_ID
AND OL_O_ID = O_ID
AND C_ID = O_C_ID
AND C_W_ID = O_W_ID
AND C_D_ID = O_D_ID
AND s_nationkey = N1.n_nationkey
AND (C_ID%25) = s_nationkey
AND (
   (N1.n_name = 'GERMANY' AND N2.n_name = 'CAMBODIA')
    OR
   (N1.n_name = 'CAMBODIA' AND N2.n_name = 'GERMANY')
    )
GROUP BY s_nationkey, SUBSTR(C_STATE,1,1), ((O_ENTRY_D/31536000)+1970)
ORDER BY s_nationkey, CUST_NATION, L_YEAR LIMIT 10;''')

    def q8(self):
        self.cursor.execute('''SELECT     ((O_ENTRY_D/31536000)+1970) AS L_YEAR,
SUM(CASE WHEN N2.n_nationkey = 7 THEN OL_AMOUNT ELSE 0.0 END) / SUM(OL_AMOUNT) AS MKT_SHARE
FROM     ITEM, supplier, STOCK, ORDER_LINE, "ORDER", CUSTOMER, nation N1, nation N2, region
WHERE     I_ID = S_I_ID
AND OL_I_ID = S_I_ID
AND OL_SUPPLY_W_ID = S_W_ID
AND ((S_W_ID * S_I_ID)%10000) = s_suppkey
AND OL_W_ID = O_W_ID
AND OL_D_ID = O_D_ID
AND OL_O_ID = O_ID
AND C_ID = O_C_ID
AND C_W_ID = O_W_ID
AND C_D_ID = O_D_ID
AND N1.n_regionkey = r_regionkey
AND OL_I_ID < 1000
AND r_name = 'EUROPE'
AND s_nationkey = N2.n_nationkey
AND I_DATA LIKE '%b'
AND I_ID = OL_I_ID
GROUP BY ((O_ENTRY_D/31536000)+1970)
ORDER BY L_YEAR LIMIT 10;''')

    def q9(self):
        self.cursor.execute('''SELECT     n_name, ((O_ENTRY_D/31536000)+1970) AS L_YEAR, SUM(OL_AMOUNT) AS SUM_PROFIT
FROM     ITEM, STOCK, supplier, ORDER_LINE, "ORDER", nation
WHERE     OL_I_ID = S_I_ID
     AND OL_SUPPLY_W_ID = S_W_ID
     AND ((S_W_ID * S_I_ID)%10000) = s_suppkey
     AND OL_W_ID = O_W_ID
     AND OL_D_ID = O_D_ID
     AND OL_O_ID = O_ID
     AND OL_I_ID = I_ID
     AND s_nationkey = n_nationkey
     AND I_DATA LIKE '%bb'
GROUP BY n_name, ((O_ENTRY_D/31536000)+1970)
ORDER BY n_name, L_YEAR DESC LIMIT 10;''')
        
#     def q10(self):
#         self.cursor.execute('''SELECT     C_ID, C_LAST, SUM(OL_AMOUNT) AS REVENUE, C_CITY, C_PHONE, n_name
# FROM     CUSTOMER, "ORDER", ORDER_LINE, nation
# WHERE     C_ID = O_C_ID
#      AND C_W_ID = O_W_ID
#      AND C_D_ID = O_D_ID
#      AND OL_W_ID = O_W_ID
#      AND OL_D_ID = O_D_ID
#      AND OL_O_ID = O_ID
#      AND O_ENTRY_D <= OL_DELIVERY_D
#      AND (C_ID%25) = n_nationkey
# GROUP BY C_ID, C_LAST, C_CITY, C_PHONE, n_name
# ORDER BY REVENUE DESC LIMIT 10;''')

    def q10(self):
        self.cursor.execute('''SELECT     C_ID, C_LAST, SUM(OL_AMOUNT) AS REVENUE, C_CITY, C_PHONE, n_name
FROM     CUSTOMER, "ORDER", ORDER_LINE, nation
WHERE     C_ID = O_C_ID
     AND C_W_ID = O_W_ID
     AND C_D_ID = O_D_ID
     AND OL_W_ID = O_W_ID
     AND OL_D_ID = O_D_ID
     AND OL_O_ID = O_ID
     AND O_ENTRY_D <= OL_DELIVERY_D
     AND C_ID = n_nationkey
GROUP BY C_ID, C_LAST, C_CITY, C_PHONE, n_name
ORDER BY REVENUE DESC LIMIT 10;''')
        
#     def q11(self):
#         self.cursor.execute('''SELECT     S_I_ID, SUM(S_ORDER_CNT) AS ORDERCOUNT
# FROM     STOCK, supplier, nation
# WHERE     ((S_W_ID * S_I_ID)%10000) = s_suppkey
#      AND s_nationkey = n_nationkey
#      AND n_name = 'GERMANY'
# GROUP BY S_I_ID
# HAVING   SUM(S_ORDER_CNT) >
#         (SELECT SUM(S_ORDER_CNT) * .005
#         FROM STOCK, supplier, nation
#         WHERE ((S_W_ID * S_I_ID)%10000) = s_suppkey
#         AND s_nationkey = n_nationkey
#         AND n_name = 'GERMANY')
# ORDER BY ORDERCOUNT DESC LIMIT 10;''')

    def q11(self):
        self.cursor.execute('''SELECT     S_I_ID, SUM(S_ORDER_CNT) AS ORDERCOUNT
FROM     STOCK, supplier, nation
WHERE     S_I_ID = s_suppkey
     AND s_nationkey = n_nationkey
     AND n_name = 'GERMANY'
GROUP BY S_I_ID
HAVING   SUM(S_ORDER_CNT) >
        (SELECT SUM(S_ORDER_CNT) * .005
        FROM STOCK, supplier, nation
        WHERE ((S_W_ID * S_I_ID)%10000) = s_suppkey
        AND s_nationkey = n_nationkey
        AND n_name = 'GERMANY')
ORDER BY ORDERCOUNT DESC LIMIT 10;''')
        
    def q12(self):
        self.cursor.execute('''SELECT     O_OL_CNT,
SUM(CASE WHEN O_CARRIER_ID = 1 OR O_CARRIER_ID = 2 THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT,
SUM(CASE WHEN O_CARRIER_ID <> 1 AND O_CARRIER_ID <> 2 THEN 1 ELSE 0 END) AS LOW_LINE_COUNT
FROM     "ORDER", ORDER_LINE
WHERE     OL_W_ID = O_W_ID
AND OL_D_ID = O_D_ID
AND OL_O_ID = O_ID
AND O_ENTRY_D <= OL_DELIVERY_D
GROUP BY O_OL_CNT
ORDER BY O_OL_CNT LIMIT 10;''')
        
    def q13(self):
        self.cursor.execute('''SELECT     C_COUNT, COUNT(*) AS CUSTDIST
FROM     (SELECT C_ID, COUNT(O_ID)
     FROM CUSTOMER LEFT OUTER JOIN "ORDER" ON (
        C_W_ID = O_W_ID
        AND C_D_ID = O_D_ID
        AND C_ID = O_C_ID
        AND O_CARRIER_ID > 8)
     GROUP BY C_ID) AS C_ORDER (C_ID, C_COUNT)
GROUP BY C_COUNT
ORDER BY CUSTDIST DESC, C_COUNT DESC LIMIT 10;''')
        
    def q14(self):
        self.cursor.execute('''SELECT    100.00 * SUM(CASE WHEN I_DATA LIKE 'pr%' THEN OL_AMOUNT ELSE 0 END) / (1+SUM(OL_AMOUNT)) AS PROMO_REVENUE
FROM ORDER_LINE, ITEM
WHERE OL_I_ID = I_ID
    LIMIT 10;''')
    
    def q15(self):
        self.cursor.execute('''WITH     REVENUE AS (
SELECT ((S_W_ID * S_I_ID)%10000) AS SUPPLIER_NO,
   SUM(OL_AMOUNT) AS TOTAL_REVENUE
FROM ORDER_LINE, STOCK
   WHERE OL_I_ID = S_I_ID AND OL_SUPPLY_W_ID = S_W_ID
GROUP BY ((S_W_ID * S_I_ID)%10000))
SELECT     s_suppkey, s_name, s_address, s_phone, TOTAL_REVENUE
FROM     supplier, REVENUE
WHERE     s_suppkey = SUPPLIER_NO
AND TOTAL_REVENUE = (SELECT MAX(TOTAL_REVENUE) FROM REVENUE)
ORDER BY s_suppkey LIMIT 10;''')
        
    def q16(self):
        self.cursor.execute('''SELECT     I_NAME,
SUBSTR(I_DATA, 1, 3) AS BRAND,
I_PRICE,
COUNT(DISTINCT (((S_W_ID * S_I_ID)%10000))) AS SUPPLIER_CNT
FROM     STOCK, ITEM
WHERE     I_ID = S_I_ID
AND I_DATA NOT LIKE 'zz%'
AND (((S_W_ID * S_I_ID)%10000) NOT IN
   (SELECT s_suppkey
    FROM supplier
    WHERE s_comment LIKE '%bad%'))
GROUP BY I_NAME, SUBSTR(I_DATA, 1, 3), I_PRICE
ORDER BY SUPPLIER_CNT DESC LIMIT 10;''')
        
    def q17(self):
        self.cursor.execute('''SELECT    SUM(OL_AMOUNT) / 2.0 AS AVG_YEARLY
FROM ORDER_LINE, (SELECT   I_ID, AVG(OL_QUANTITY) AS A
            FROM     ITEM, ORDER_LINE
            WHERE    I_DATA LIKE '%b'
                 AND OL_I_ID = I_ID
            GROUP BY I_ID) T
WHERE OL_I_ID = T.I_ID
    AND OL_QUANTITY < T.A LIMIT 10;''')
        
    def q18(self):
        self.cursor.execute('''SELECT     C_LAST, C_ID, O_ID, O_ENTRY_D, O_OL_CNT, SUM(OL_AMOUNT)
FROM     CUSTOMER, "ORDER", ORDER_LINE
WHERE     C_ID = O_C_ID
     AND C_W_ID = O_W_ID
     AND C_D_ID = O_D_ID
     AND OL_W_ID = O_W_ID
     AND OL_D_ID = O_D_ID
     AND OL_O_ID = O_ID
GROUP BY O_ID, O_W_ID, O_D_ID, C_ID, C_LAST, O_ENTRY_D, O_OL_CNT
HAVING     SUM(OL_AMOUNT) > 200
ORDER BY SUM(OL_AMOUNT) DESC, O_ENTRY_D LIMIT 10;''')
        
    def q19(self):
        self.cursor.execute('''SELECT    SUM(OL_AMOUNT) AS REVENUE
FROM ORDER_LINE, ITEM
WHERE    (
      OL_I_ID = I_ID
          AND I_DATA LIKE '%a'
          AND OL_QUANTITY >= 1
          AND OL_QUANTITY <= 10
          AND I_PRICE BETWEEN 1 AND 400000
          AND OL_W_ID IN (1,2,3)
    ) OR (
      OL_I_ID = I_ID
      AND I_DATA LIKE '%b'
      AND OL_QUANTITY >= 1
      AND OL_QUANTITY <= 10
      AND I_PRICE BETWEEN 1 AND 400000
      AND OL_W_ID IN (1,2,4)
    ) OR (
      OL_I_ID = I_ID
      AND I_DATA LIKE '%c'
      AND OL_QUANTITY >= 1
      AND OL_QUANTITY <= 10
      AND I_PRICE BETWEEN 1 AND 400000
      AND OL_W_ID IN (1,5,3)
    ) LIMIT 10;''')
        
    def q20(self):
        self.cursor.execute('''SELECT   s_name, s_address
FROM     supplier, nation
WHERE    s_suppkey IN
        (SELECT  ((S_I_ID * S_W_ID)%10000)
        FROM     STOCK, ORDER_LINE
        WHERE    S_I_ID IN
                (SELECT I_ID
                 FROM ITEM
                 WHERE I_DATA LIKE 'co%')
             AND OL_I_ID=S_I_ID
        GROUP BY S_I_ID, S_W_ID, S_QUANTITY
        HAVING   2*S_QUANTITY > SUM(OL_QUANTITY))
     AND s_nationkey = n_nationkey
     AND n_name = 'GERMANY'
ORDER BY s_name LIMIT 10;''')
        
#     def q21(self):
#         self.cursor.execute('''SELECT     s_name, COUNT(*) AS NUMWAIT
# FROM     supplier, ORDER_LINE L1, "ORDER", STOCK, nation
# WHERE     OL_O_ID = O_ID
#      AND OL_W_ID = O_W_ID
#      AND OL_D_ID = O_D_ID
#      AND OL_W_ID = S_W_ID
#      AND OL_I_ID = S_I_ID
#      AND ((S_W_ID * S_I_ID)%10000) = s_suppkey
#      AND L1.OL_DELIVERY_D > O_ENTRY_D
#      AND NOT EXISTS (SELECT *
#              FROM ORDER_LINE L2
#              WHERE  L2.OL_O_ID = L1.OL_O_ID
#                 AND L2.OL_W_ID = L1.OL_W_ID
#                 AND L2.OL_D_ID = L1.OL_D_ID
#                 AND L2.OL_DELIVERY_D > L1.OL_DELIVERY_D)
#      AND s_nationkey = n_nationkey
#      AND n_name = 'GERMANY'
# GROUP BY s_name
# ORDER BY NUMWAIT DESC, s_name LIMIT 10;''')

    def q21(self):
        self.cursor.execute('''WITH max_delivery AS (
    SELECT OL_O_ID, OL_W_ID, OL_D_ID, MAX(OL_DELIVERY_D) AS MAX_DELIVERY_D
    FROM ORDER_LINE
    GROUP BY OL_O_ID, OL_W_ID, OL_D_ID
)
SELECT     s_name, COUNT(*) AS NUMWAIT
FROM     supplier, ORDER_LINE L1, "ORDER", STOCK, nation, max_delivery MD
WHERE    L1.OL_O_ID = O_ID
     AND L1.OL_W_ID = O_W_ID
     AND L1.OL_D_ID = O_D_ID
     AND L1.OL_W_ID = S_W_ID
     AND L1.OL_I_ID = S_I_ID
     AND S_I_ID = s_suppkey 
     AND L1.OL_DELIVERY_D > O_ENTRY_D
     AND L1.OL_O_ID = MD.OL_O_ID
     AND L1.OL_W_ID = MD.OL_W_ID
     AND L1.OL_D_ID = MD.OL_D_ID
     AND L1.OL_DELIVERY_D = MD.MAX_DELIVERY_D
     AND s_nationkey = n_nationkey
     AND n_name = 'GERMANY'
GROUP BY s_name
ORDER BY NUMWAIT DESC, s_name LIMIT 10;''')
        
    def q22(self):
        self.cursor.execute('''SELECT     SUBSTR(C_STATE,1,1) AS COUNTRY,
COUNT(*) AS NUMCUST,
SUM(C_BALANCE) AS TOTACCTBAL
FROM     CUSTOMER
WHERE     SUBSTR(C_PHONE,1,1) IN ('1','2','3','4','5','6','7')
AND C_BALANCE > (SELECT AVG(C_BALANCE)
         FROM      CUSTOMER
         WHERE  C_BALANCE > 0.00
             AND SUBSTR(C_PHONE,1,1) IN ('1','2','3','4','5','6','7'))
AND NOT EXISTS (SELECT *
        FROM    "ORDER"
        WHERE    O_C_ID = C_ID
                AND O_W_ID = C_W_ID
               AND O_D_ID = C_D_ID)
GROUP BY SUBSTR(C_STATE,1,1)
ORDER BY SUBSTR(C_STATE,1,1) LIMIT 10;''')        


# tpcc_instance = TPCC(num_warehouse=num_warehouse, port='5432', time=30)
# tpcc_instance.run()
# tpch_instance = TPCH(num_warehouse=num_warehouse, port='5432', time=360)
# tpch_instance.run()

shared_queue = queue.Queue()

# Create TPCC and TPCH instances
tpcc_instances = [TPCC(num_warehouse=num_warehouse, port=port, time=run_time, queue=shared_queue, id=i) for i in range(num_tpcc_threads)]
tpch_instances = [TPCH(num_warehouse=num_warehouse, port=port, time=run_time, queue=shared_queue, id=i) for i in range(num_tpch_threads)]


# Create threads for TPCC
tpcc_threads = [threading.Thread(target=tpcc_instance.run) for tpcc_instance in tpcc_instances]

# Create threads for TPCH

tpch_threads = [threading.Thread(target=tpch_instance.run) for tpch_instance in tpch_instances]

# Start the threads for TPCC
for tpcc_thread in tpcc_threads:

    tpcc_thread.start()

# Start the threads for TPCH
for tpch_thread in tpch_threads:
    tpch_thread.start()

# Wait for all TPCC threads to finish
for tpcc_thread in tpcc_threads:
    tpcc_thread.join()

# Wait for all TPCH threads to finish
for tpch_thread in tpch_threads:
    tpch_thread.join()

all_result = list(shared_queue.queue)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")  # "2024-10-09_14-30"
with open(f"result_{timestamp}.json", "w") as f:
    json.dump(all_result, f)