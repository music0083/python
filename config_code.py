import sqlite3
from concurrent.futures import ThreadPoolExecutor
import tushare as ts


ts.set_token("1479293fc1f943036977bbe9c2f0d13e9e8e8951677540465b7da738")
pro = ts.pro_api('1479293fc1f943036977bbe9c2f0d13e9e8e8951677540465b7da738')

#获取深圳日线
def get_stock_day_line_data_for_SZSE(stock_name):
   
    conn = sqlite3.connect("SZSE_day_line_data.db",check_same_thread=False)
   
    try:
        df = pro.daily(ts_code=stock_name, start_date='20150101')
        #存入对应数据库的日线表
        df.to_sql(str(stock_name),conn,if_exists='replace')
        print(stock_name+":is ok")
    
    except:
        error.append(stock_name)      
        return stock_name

    conn.commit()
    conn.close()

#获取上证日线1
def get_stock_day_line_data_for_SSE(stock_name):
       
    conn = sqlite3.connect("SSE_day_line_data.db",check_same_thread=False)

    try:
        df = pro.daily(ts_code=stock_name, start_date='20150101')
        #存入对应数据库的日线表
        df.to_sql(str(stock_name),conn,if_exists='replace')
        print(stock_name+":is ok")
    
    except:
        error.append(stock_name) 
        return stock_name

    conn.commit()
    conn.close()



#获取上市股票列表
def get_stock_basic_list():
    db_name_shenzheng='SZSE.db'#深证交易所
    db_name_shangzheng='SSE.db'#上海交易所
    conn_SZSE = sqlite3.connect(db_name_shenzheng)
    conn_SSE  = sqlite3.connect(db_name_shangzheng)
    data_SZSE = pro.stock_basic(exchange='SZSE', list_status='L', fields='ts_code,symbol,name,area,industry,list_date,is_hs,market')
    data_SSE = pro.stock_basic(exchange='SSE', list_status='L', fields='ts_code,symbol,name,area,industry,list_date,is_hs,market')
    data_SZSE.to_sql('SZSE',conn_SZSE,if_exists='replace')
    data_SSE.to_sql('SSE',conn_SSE,if_exists='replace')


#历史分笔
def get_code_history_day_trade_data(x_code,x_date):
    db_name=x_code+'.db'
    conn = sqlite3.connect(db_name)
    df = ts.get_tick_data(code=x_code,date=x_date,src='tt')
    df.to_sql('tick_data',conn,if_exists='replace')
    

#获取上证清单
def get_SSE_list():
        conn = sqlite3.connect('SSE.db')
        sql = "select ts_code from SSE"
        c = conn.cursor()
        c.execute(sql)
        values = c.fetchall()
        list = []
        for i in values:
            list.append(i[0])
        return list

#获取深证清单
def get_SZSE_list():
        conn = sqlite3.connect('SZSE.db')
        sql = "select ts_code from SZSE"
        c = conn.cursor()
        c.execute(sql)
        values = c.fetchall()
        list = []
        for i in values:
            list.append(i[0])
        return list


#指数获取
    # #上证指数
    # ts.get_hist_data('sh')
    # #获取沪深300指数k线数据
    # ts.get_hist_data('hs300'）
    # #获取上证50指数k线数据
    # ts.get_hist_data('sz50')
    # #获取中小板指数k线数据
    # ts.get_hist_data('zxb')
    # #获取创业板指数k线数据
    # ts.get_hist_data('cyb')
    # #深证指数
    # ts.get_hist_data('sz'）

if __name__=="__main__" :
    list_sse=get_SSE_list()
    list_szse=get_SZSE_list()
    conn = sqlite3.connect("SSE_day_line_data.db",check_same_thread=False)
    # with ThreadPoolExecutor(max_workers=2) as pool:
    # # 使用线程执行map计算
    # # 后面元组有3个元素，因此程序启动3条线程来执行action函数
    #     r= pool.map(get_stock_day_line_data_for_SSE, list_sse)
    #     print('--------------')
    #     for i in r:
    #         print(i)
    with ThreadPoolExecutor(max_workers=2) as pool:
    # 使用线程执行map计算
    # 后面元组有3个元素，因此程序启动3条线程来执行action函数
        r= pool.map(get_stock_day_line_data_for_SZSE, list_szse)
        print('--------------')
        for i in r:
            print(i)    
    print(error)
 