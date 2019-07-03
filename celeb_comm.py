import pandas as pd
from sqlalchemy import create_engine
import datetime
from functools import reduce
import os
from pathlib import Path

#start_date = '2019-04-05'
#end_date = '2019-04-13'

def get_date(which):
	date_text = input('Input ' + which + ' Date (YYYY-MM-DD):')
	try:
		valid_date = datetime.datetime.strptime(date_text, '%Y-%m-%d').date()
		return valid_date
	except ValueError:
		print("Invalid Date. Please input a valid date int the format YYYY-MM-DD")
		return get_date(which)



uri = 'mysql://readonly:ZRxtxJCR3nLlNYgA@54.171.238.241/aoi?charset=utf8'
engine = create_engine(uri, encoding="utf8")
conn = engine.connect()

start_date = str(get_date('Start'))
end_date = str(get_date('End'))

returned_query = """SELECT od.sku, od.sku_name, od.celebrity_id, od.celebrity_name, SUM(CASE WHEN od.order_date BETWEEN '""" + start_date + """' and '""" + end_date + """' then od.quantity else 0 end) returned_current_qty, SUM(CASE WHEN od.order_date BETWEEN '""" + start_date + """' and '""" + end_date + """' then od.net_sale_price_kwd else 0 end) returned_current_amount, SUM(CASE WHEN od.order_date < '""" + start_date + """'  then od.quantity else 0 end) returned_prior_qty, SUM(CASE WHEN od.order_date < '""" + start_date + """'  then od.net_sale_price_kwd else 0 end) returned_prior_amount FROM OFS.PostedReturnQC qc LEFT JOIN aoi.order_details od on od.item_id=qc.ItemId where date(qc.InsertedOn) BETWEEN '""" + start_date + """' and '""" + end_date + """' and lower(od.order_category) !='celebrity' and od.celebrity_id !='NULL' group by od.sku, od.sku_name, od.celebrity_id """
cancelled_query = """SELECT od.sku, od.sku_name, od.celebrity_id, od.celebrity_name, SUM(CASE WHEN od.order_date BETWEEN '""" + start_date + """' and '""" + end_date + """' then od.quantity else 0 end) cancelled_current_qty, SUM(CASE WHEN od.order_date BETWEEN '""" + start_date + """' and '""" + end_date + """' then od.net_sale_price_kwd else 0 end) cancelled_current_amount, SUM(CASE WHEN od.order_date < '""" + start_date + """'  then od.quantity else 0 end) cancelled_prior_qty, SUM(CASE WHEN od.order_date < '""" + start_date + """'  then od.net_sale_price_kwd else 0 end) cancelled_prior_amount FROM OFS.CancelledOrders co LEFT JOIN aoi.order_details od on co.ItemId=od.item_id where date(co.CancelledDate) BETWEEN '""" + start_date + """' and '""" + end_date + """' and lower(od.order_category) !='celebrity' and od.celebrity_id !='NULL' group by od.sku, od.sku_name, od.celebrity_id """
total_query = """select sku, sku_name, celebrity_id, celebrity_name, sum(quantity) total_qty, sum(net_sale_price_kwd) total_amount from (select * from order_details where celebrity_id is not NULL and order_category <> 'CELEBRITY' and order_date between '""" + start_date + """' and '""" + end_date + """')a group by 1,2,3"""
paid_delivered_query = """select sku, sku_name, celebrity_id, celebrity_name, sum(quantity) paid_delivered_qty, sum(net_sale_price_kwd) paid_delivered_amount from (select * from  order_details where celebrity_id is not NULL and order_category <>  'CELEBRITY' and order_date between '""" + start_date + """' and '""" + end_date + """' and (case when  order_currency <> 'KWD' then date(shipped_at) else date(delivered_at) end) between '""" + start_date + """' and  '""" + end_date + """') a group by 1,2,3"""
paid_delivered_prior_query = """select sku, sku_name, celebrity_id, celebrity_name, sum(quantity) paid_delivered_prior_qty, sum(net_sale_price_kwd) paid_delivered_prior_amount from (select * from order_details where celebrity_id is not NULL and order_category <> 'CELEBRITY' and order_date < '""" + start_date + """' and (case when order_currency <> 'KWD' then date(shipped_at) else  date(delivered_at) end) between '"""+ start_date + """' and '""" + end_date + """')  a group by 1,2,3"""

total = pd.read_sql(total_query, conn)
returned = pd.read_sql(returned_query, conn)
cancelled = pd.read_sql(cancelled_query, conn)
paid_delivered = pd.read_sql(paid_delivered_query , conn)
paid_delivered_prior = pd.read_sql(paid_delivered_prior_query , conn)

dfs = [total, cancelled, returned, paid_delivered, paid_delivered_prior, ]
df_final = reduce(lambda left,right: pd.merge(left,right, how='outer'), dfs)
df_final = df_final.fillna(0)
#df_final


df_final['net_qty'] = df_final['total_qty'] - df_final['returned_current_qty'] - df_final['cancelled_current_qty']
df_final['process_qty'] = df_final['net_qty'] - df_final['paid_delivered_qty']
df_final['unit_price'] = df_final['paid_delivered_amount']/df_final['paid_delivered_qty']
df_final['process_qty'] = df_final['net_qty'] - df_final['paid_delivered_qty']
df_final['commision_amount'] = df_final['paid_delivered_amount']*0.1 
df_final['commision_amount_prior'] = (df_final['paid_delivered_prior_amount'] -df_final['returned_prior_amount'])*0.1
df_final['total_commision']  = df_final['commision_amount_prior'] + df_final['commision_amount']
df_final = df_final.fillna(0)

home = str(Path.home())

csv_location = str(Path.home()) + '/celeb_comm_' + start_date + '_' + end_date + '.csv'
df_final.to_csv(csv_location, index=False)
print("Saving to the location: " + csv_location)


