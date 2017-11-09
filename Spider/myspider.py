import requests
import urllib
import os
import re
import time
import pymysql
import threading
import random
import socket
import configparser
import sys
import warnings

warnings.filterwarnings("ignore")

websuccess = 0
webfail = 0
linknum = 0

def getnum(target):
    if target == 'websuccess':
        global websuccess
        return websuccess
    elif target == 'webfail':
        global webfail
        return webfail
    else:
        global linknum
        return linknum

def addnum(target):
    if target == 'websuccess':
        global websuccess
        websuccess += 1
    elif target == 'webfail':
        global webfail
        webfail += 1
    else:
        global linknum
        linknum += 1
    return

def read_cw_config():
    config_file_path = 'config.ini'
    cf = configparser.ConfigParser()
    cf.read(config_file_path)
    max_jobnum = cf.getint('crawler', 'max_jobnum')
    init_url = cf.get('crawler', 'init_url')
    return max_jobnum, init_url

def read_db_config():
    config_file_path = 'config.ini'
    cf = configparser.ConfigParser()
    cf.read(config_file_path)
    host = cf.get('database', 'host')
    port = cf.getint('database', 'port')
    user = cf.get('database', 'user')
    passwd = cf.get('database', 'password')
    db = cf.get('database', 'db_name')
    charset = cf.get('database', 'charset')
    return host, port, user, passwd, db, charset

# 连接数据库
def connect_db():
    host, port, user, passwd, db, charset = read_db_config()
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset=charset)
    c = conn.cursor()
    return conn, c

# 初始化子域名数据表
def init_table(url, conn, c):
    loc = getloc(url).lower()
    scheme = getsch(url)
    ctime = time.time()
    
    result = c.execute("select * from _url_list where loc='%s' limit 0,1" % loc)
    if not result:
        c.execute("create table if not exists `%s` (id INTEGER auto_increment , path TEXT, schele TEXT, search INTEGER, search_finished INTEGER, \
            download_finished INTEGER, create_at INTEGER, latest_at INTEGER, primary key(id)) character set = utf8" % (loc))
        c.execute("insert into `%s` (schele, path, search, create_at) values ('%s', '/', 0, '%s')" % (loc, scheme, ctime))
        conn.commit()
        c.execute("insert into _url_list (loc, masking, level, finished, create_at) values ('%s', 0, 0, 0, '%s')" % (loc, ctime))
        conn.commit()        
    return True

def get_all_list(conn, c):
    c.execute('select loc from _url_list where finished = 0 order by id')
    all_list = list(c.fetchall())
    conn.commit()
    return all_list

# 获取并下载网页
def geturl(url):
    try:
        req = requests.get(url, timeout=5)
        req.encoding = 'utf-8'
        back = req.text
        addnum('websuccess')
        return back
    except:
        addnum('webfail')
        return False
    

def writefile(url, content):
    loc = urllib.parse.quote_plus(getloc(url))
    urlencode = urllib.parse.quote_plus(url)
    cdir = 'f:/web-crawler/search-engine/Spider/download/'+loc
    savepath = cdir+'/'+urlencode+'_'+str(random.randint(1, 1000))
    if not os.path.exists(cdir):
        os.makedirs(cdir)
    try:    
        with open(savepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    except:
        #print(url + '文件写入失败')
        return False

# 保存网址到数据库
def save_to_db(scheme, loc, path, conn, c):
    ctime = time.time()
    c.execute("select * from _url_list where loc='%s'")
    result = c.fetchall()
    if not result:
        init_table(scheme+'://'+loc, conn, c)
    c.execute("SELECT * FROM `%s` where path='%s'" % (loc, path))
    exist = c.fetchall()
    for row in exist:
        return
    c.execute("insert into `%s` (path, schele, search, create_at) values ('%s', '%s', 0, '%s')" % (loc, path, scheme, ctime))
    conn.commit()
    addnum('linknum')
    return

# 设置状态
def set_status(path, target, loc, conn, c):
    if not path:
        path = '/'
    c.execute("UPDATE `%s` set `%s` = 1 where path='%s'" % (loc, target, path))
    conn.commit()
    return

def get_loc_nums(target, conn, c):
    if target == 'all':
        c.execute("select * from _url_list")
        loc_nums = len(list(c.fetchall()))
    elif target == "finished":
        c.execute("select * from _url_list where finished=1")
        loc_nums = len(list(c.fetchall()))
    else:
        return False
    conn.commit()
    return loc_nums

def getloc(url):
    return urllib.parse.urlparse(url).netloc
def getsch(url):
    return urllib.parse.urlparse(url).scheme
def getpath(url):
    return urllib.parse.urlparse(url).path

def crawl_single_url(scheme_pre, loc_pre, path_pre, conn, c):
    suffix_list = ['htm', 'html', 'shtml', 'stm', 'shtm', 'asp', 'htm', 'html', 'shtml', 'stm', 'shtm', 'asp']
    set_status(path_pre, 'search', loc_pre, conn, c)
    url_pre = scheme_pre+'://'+loc_pre+path_pre
    back = geturl(url_pre)
    if not back:
        return
    s = "<a.+href=[\'\"](\S+?)[\'\"].*>"
    links = re.findall(s, back)
    for link in links:
        ifspecail = re.findall('#|mailto|javascript|mms:|ftp:|@', link)
        if ifspecail:
            continue
        scheme = getsch(link)
        if not scheme:
            scheme = 'http'
        loc = getloc(link)
        if not loc:
            loc = loc_pre
        if loc.find('dlut.edu.cn') < 0:
            continue
        path = getpath(link)
        lastname = path.split('/')[-1]
        if lastname:
            suffix = lastname.split('.')[-1]
            if suffix:
                if suffix not in suffix_list:
                    continue
        if path.startswith('/../'):
            path = path[1:]
        if path.startswith('../'):
            path = path[2:]
            tmp = '/'.join(path_pre.split('/')[:-2])
            if path.startswith('/../'):
                path = path[3:]
                tmp = '/'.join(tmp.split('/')[:-2])
                if path.startswith('/../'):
                    path = path[3:]
                    tmp = '/'.join(tmp.split('/')[:-2])
            path = tmp+path
        if not path.startswith('/'):
            path = './'+path
        if path.startswith('./'):
            path = path[1:]
            tmp = '/'.join(path_pre.split('/')[:-1])
            path = tmp+path
        save = save_to_db(scheme, loc.lower(), path, conn, c)

    set_status(path_pre, 'search_finished', loc_pre, conn, c)
    result = writefile(url_pre, back)
    if result:
        set_status(path_pre, 'download_finished', loc_pre, conn, c)
    return

def start_one_thread(init_loc):
    conn, c = connect_db()
    atime = time.time()
    while(1):
        btime = time.time()
        if btime-atime > 120:
            break
        c.execute("select * from `%s` where search=0 order by id limit 0,1" % init_loc)
        nexturls = c.fetchall()
        if nexturls:
            for nexturl in nexturls:
                path = nexturl[1]
                scheme = nexturl[2]
                crawl_single_url(scheme, init_loc, path, conn, c)
        else:
            #print('\n%s子域名下爬取完毕！\t\t\t\t\t\t\t\t' % init_loc, end='\r')
            c.execute("UPDATE _url_list set finished = 1 where loc='%s'" % (init_loc))
            conn.commit()
            break
    conn.close()
    return
