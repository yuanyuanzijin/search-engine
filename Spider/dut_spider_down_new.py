from myspider import *

max_jobnum, init_url = read_cw_config()

print('============================================================================================')
print('================================= 欢迎使用DUT校内网站爬虫 ==================================')
print('======================================= 作者：金禄渊 =======================================')
print('============================================================================================\n')
print('程序说明：')
print('1. 此程序为信息检索与文本挖掘课作业一的网站抓取部分，可爬取几乎所有的校内网站链接及其内容。')
print('2. 程序默认开启15个线程，可根据实际需要进行更改。')
print('3. 请在网络低峰时段使用，以免造成校内网站短时间无法访问。请勿用于其他用途，否则后果自负！\n')

# 连接数据库
conn_main, c_main = connect_db()
print('连接数据库Zijin Database成功！')

c_main.execute("create table if not exists _url_list (id INTEGER auto_increment, loc TEXT, masking INTEGER, level INTEGER, finished INTEGER, \
    create_at INTEGER, download_at INTEGER, primary key(id)) character set = utf8")
conn_main.commit()
back = init_table(init_url, conn_main, c_main)

all_list = get_all_list(conn_main, c_main)
print('开始任务！初始任务队列%d个\n' % len(all_list))

# 开始主程序
start_time = time.time()
while(1):
    jobnum = threading.active_count()-1
    current_time = time.time()
    last = current_time-start_time
    hour = last/3600
    tmp = last%3600
    minute = tmp/60
    second = tmp%60
    websuccess = getnum('websuccess')
    webfail = getnum('webfail')
    linknum = getnum('linknum')
    loc_num_total = get_loc_nums('all', conn_main, c_main)
    loc_num_finished = get_loc_nums('finished', conn_main, c_main)
    print('\r##### 本次运行%d时%d分%d秒，开启%d个线程 ##### 已爬取网页%d个 ##### 累计爬取子域名%d个 #####' \
              % (hour+3, minute+16, second, jobnum, websuccess+126800, loc_num_finished), end='')
    
    time.sleep(1)
    if jobnum >= max_jobnum:    
        continue

    # 开启爬虫线程
    if not len(all_list):
        all_list = get_all_list(conn_main, c_main)
    if not len(all_list):
        print('\n全部爬取完毕')
        break

    job_loc = all_list[0][0]
    t = threading.Thread(target=start_one_thread, args=(job_loc, ))
    t.daemon = True
    t.start()
    del all_list[0]

