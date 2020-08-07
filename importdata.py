# -*- coding: gb2312 -*-
#python -m py_compile "d:\cib\importdata.py"

'''
经过运行一段时间，发现下面问题：
1、下载的文件的字段多，备注多或其他列多了 ","号，导致列多，此时修改文件即可
2、出现重复导入的情况，删除重复的文件即可
3、在将数据导入正式表时出现错误，结果没有日志，修改 2019/10/30

'''

import csv
import sys
import os
import time
import pandas as pd
import numpy as np
from pandas import DataFrame,Series
import cx_Oracle
import smtplib  
from email.mime.text import MIMEText  
from email.header import Header 
import logging
import colorama
from colorama import init,Fore,Back,Style
init(autoreset=True)

cib={
"lck_guahao_jiuyi":"D:/cib/理财卡/预约挂号/",
"lck_tijian_jieya":"D:/cib/理财卡/体检洁牙/",
"xyk_jiuyi":"D:/cib/信用卡/天谷源-贵宾就医/",
"xyk_jieya":"D:/cib/信用卡/天谷源-洁牙/",
"xyk_tianjian":"D:/cib/信用卡/天谷源-体检/",
"xyk_guahao":"D:/cib/信用卡/天谷源-专家预约/",
"sh_guibin_jiuyi":"D:/cib/私行/贵宾就医/",
"lck_team":"D:/cib/团检/",
}

def myoutput(msg):
    logger = logging.getLogger("importdata") 
    logger.info(msg)
    print(msg)

#发送邮件    
def send_mail(subject,content,tolist=None):
    smtpserver = 'smtp.exmail.qq.com'  
    smtpport = 465
    username = 'log.sys@tianguyuan.com'  
    password = 'Tian2017#'
    sender = 'log.sys@tianguyuan.com'  
    if tolist == None :
        tolist = ["jialai.hao@tianguyuan.com","pingfan.zhu@tianguyuan.com","xia.chen@tianguyuan.com","liulei.lv@tgene.com.cn"]

    try:
        msg = MIMEText(content,'html','utf-8')#中文需参数‘utf-8’，单字节字符不需要  
        msg['Subject'] = Header(subject, 'utf-8')  
        msg['From'] = sender #("%s<"+sender+">"%Header(sender, 'utf-8'),)
        msg['To'] = ",".join(tolist)
        smtp = smtplib.SMTP_SSL(smtpserver,smtpport)  
        smtp.connect(smtpserver,smtpport)  
        smtp.ehlo()  
        #smtp.starttls()  
        #smtp.ehlo()  
        smtp.set_debuglevel(0)  
        smtp.login(username, password)  
        smtp.sendmail(sender, tolist, msg.as_string())  
        smtp.quit() 
        return 0
    except Exception as ex:
        myoutput(ex)
        return -1
        
'''
oracle连接
'''
def test_orcle():
    host = "192.168.1.247"   #数据库ip
    port = "1521"        #端口
    sid = "orcl"         #数据库名称
    dsn = cx_Oracle.makedsn(host, port, sid)
    conn = cx_Oracle.connect("tgycrm", "Tianguyuan123#", dsn)
    #conn = cx_Oracle.connect("tgycrmdev", "Tianguyuan123#", dsn)
    sql = 'select sysdate from dual'
    rst = pd.read_sql(sql,conn)
    return conn

'''
检查目录文件是否存在,不存在则需要创建
'''    
def checkpath(): 
    list = cib.keys();
    for key in cib.keys():
        if os.path.exists(cib[key]):
            myoutput(cib[key]+" 目录存在")
        else:
            os.makedirs(cib[key])
            if os.path.exists(cib[key]):
                myoutput(cib[key]+" 目录存在")
            else:
                myoutput(cib[key]+" 目录创建失败")
                return False

'''
获取目录下的所有.csv的文件,如果目录没有自动创建
'''
def get_filelist(path):
    list = None
    if not os.path.exists(path):
        os.makedirs(path)
        if not os.path.exists(path):
            myoutput(cib[key]+" 目录创建失败")
            return list
            
    list = os.listdir(path)
    return list
    
    #if not os.path.exists(path):
    #    myoutput(path+" 无目录")
    #    return list
    #else:
    #    list = os.listdir(path)
    #    return list

'''
检查文件是否已经处理,如果处理了则不需要处理
'''
def checkcompleted(cursor,file):
    cursor.prepare("select count(*) num from tgy_xingye_import_his where filepath=:filepath")
    cursor.execute(None,{'filepath':file})
    #获取一条记录
    one = cursor.fetchone()
    if one[0]>0 :        
        return True
    return False

def checkcompleted_team(cursor,file):
    cursor.prepare("select count(*) num from tgy_xingye_team_import_his where filepath=:filepath")
    cursor.execute(None,{'filepath':file})
    #获取一条记录
    one = cursor.fetchone()
    if one[0]>0 :        
        return True
    return False    

'''
保存处理完成的文件,一般未路径+file的方式
存储了保存的时间和
'''
def savecompleted(cursor,file):
    cursor.execute("insert into tgy_xingye_import_his(filepath) values(:filepath)",{"filepath":file})
    return True
    
def savecompleted_team(cursor,file):
    cursor.execute("insert into tgy_xingye_team_import_his(filepath) values(:filepath)",{"filepath":file})
    return True    
    
'''
获取需要处理的文件列表
'''
def get_todeal_list(path):
    l = []
    flist = get_filelist(path)
    #如果目录不存在则直接返回
    if flist is None:
        return l
    
    #如果没有文件则不进行处理
    if len(flist)<1 :
        myoutput(path+" 无文件需要处理")
        return l
   
    conn = test_orcle()
    cur = conn.cursor()
    for f in flist:
        #检查是否已经处理,如果已经处理则不需要处理
        if checkcompleted(cur,path+f):
            myoutput(path+f+" 已经导入")
            continue
        l.append(path+f)
    cur.close()
    conn.close()
    
    #排序保证文件按日期顺序处理
    l.sort()
    return l
    
def get_todeal_list_team(path):
    l = []
    flist = get_filelist(path)
    #如果目录不存在则直接返回
    if flist is None:
        return l
    
    #如果没有文件则不进行处理
    if len(flist)<1 :
        myoutput(path+" 无文件需要处理")
        return l
    
    conn = test_orcle()
    cur = conn.cursor()
    for f in flist:
        #检查是否已经处理,如果已经处理则不需要处理
        if checkcompleted_team(cur,path+f):
            myoutput(path+f+" 已经导入")
            continue
        l.append(path+f)
    cur.close()
    conn.close()
    #排序保证文件按日期顺序处理
    l.sort()
    return l    
 
'''
撤销订单处理,撤销订单是由调用过程管理事务是否同时完成
当第二次撤销此订单时，订单已经被撤销的认为成功
cursor 为游标
如果是导入状态的订单取消是什么情况呢?
如果没有导入
'''
def deal_cancel(cursor,df):
    if df is None or len(df)<1:
        return True  
    #根据订单号取消订单，取消时有备注
    for i,row in df.iterrows():
        bank_no = row["订单编号"]
        #调研存储过程取消订单
        #执行存储过程,将数据导入主表
        ord_id = cursor.var(cx_Oracle.NUMBER)
        cursor.callproc("TGY_XINGYE_ALL_DATA_CANCEL",["SYSTEM",bank_no,ord_id])        
        #如果错误呢,返回False
    return True  
    
'''
生成团检单文件,采用覆盖的方式
'''
def create_team_file(df,filepath):    
    if len(df)<1:
        return True
        
    path = cib["lck_team"]+time.strftime("%Y%m%d", time.localtime())+"/"
    if os.path.exists(path) is False:            
        os.makedirs(path)
        if os.path.exists(path) is False:
            myoutput(cib[key]+" 目录创建失败")
            return False
    filename = path+os.path.basename(filepath)
    rt = df.to_csv(filename,index=False,encoding="gbk") 
    #print("---------team team team ---------")
    #print("有团检文件，请进行团检文件导入操作"+filename)
    #print("---------team team team ---------")
    return True
    
    
'''
理财卡的预约挂号数据导入系统
理财卡的预约挂号数据的订单详情数据是['':'','':'']方式,需要更改为["":"";"":""]方式
'''    
def deal_lck(file):  
           
    myoutput("处理: "+file)    
    dataline = list(csv.reader(open(file, 'r')))
    f=open(file, 'r')   
    lines=f.readlines()
    f.close()
    #处理字符串格式转换
    for i in range(1,len(lines)):
        s = lines[i].strip()
        l = s.find('[')
        m = s.find(']',l)
        subs = s[l:m+1]
        subs = subs.replace(",",";")
        subs = subs.replace("'",'"') 
        #截取l之前+含[和]的字符串+]之后的字符串    
        line = s[:l]+subs+s[(m+1):]        
        dataline[i]=line.split(",") 
        
    df=pd.DataFrame(dataline[1:],columns=dataline[0])
    cxdf = df[df["订单状态"].isin(["预约撤销","预约撤消"])].copy()
    cxdf = cxdf[["订单编号","订单状态"]]
    cxdf=cxdf.reset_index()
    df = df[~df["订单状态"].isin(["预约撤销","预约撤消"])].copy()    
    #过滤是否有团检,如果有生成csv文件为了后续导入
    df["Team"] = df["备注"].apply(lambda x:x.find("团检"))
    dfteam = df[df["Team"]>-1].copy()    
    #如果有团检生成团检文件
    if len(dfteam)>0:
        dfteam=dfteam.reset_index(drop=True)
        if create_team_file(dfteam,file) is False:
            return False
    
    df = df[df["Team"]<0].copy()
    df=df.reset_index()
    
    #df1 = pd.read_csv( path+f,',',encoding="gb2312",dtype=coltype) #直接读文件一直出现错误
	#理财卡 预约挂号模板
    sql = "insert into tgy_xingye_import(订单编号,客户身份,姓名,手机号,行权日期,行权时间,是否租杆,订单状态,证件号码,服务项目,服务供应商,订单详情,备注)" 
    sql=sql+" values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
	
    conn = test_orcle()
    cur = conn.cursor()
    
    try:
        cur.execute("alter session set nls_date_format='yyyymmdd'")
        for i,row in df.iterrows():
            #print(i,len(df))
            cur.execute(sql%(row["订单编号"],row["客户身份"],row["姓名"],row["手机号"],row["行权日期"]
					,row["行权时间"],row["是否租杆"],row["订单状态"],row["证件号码"],row["服务项目"]
					,row["服务供应商"],row["订单详情"],row["备注"]))
            
        #for i in range(len(df)):
        #    cur.execute(sql%(df.at[i,"订单编号"],df.at[i,"客户身份"],df.at[i,"姓名"],df.at[i,"手机号"],df.at[i,"行权日期"]
        #        ,df.at[i,"行权时间"],df.at[i,"是否租杆"],df.at[i,"订单状态"],df.at[i,"证件号码"],df.at[i,"服务项目"]
        #        ,df.at[i,"服务供应商"],df.at[i,"订单详情"],df.at[i,"备注"]))
        
        ##一次插入多条数据,参数为字典列表形式
        #param=[{'id':3,'n':'admin','p':'password'},{'id':4,'n':'admin','p':'password'},{'id':5,'n':'admin','p':'password'}];
        #cursor.executemany('insert into tb_user values(:id,:n,:p)',param);
        #保存文件导入记录
        savecompleted(cur,file)        
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        conn.rollback()
        cur.close()
        conn.close()
        return False        
    conn.commit()
    
    '''
    同一批文件中不会出现先有订单后又取消此订单的情况，所以取消订单先处理    
    '''
    #撤销订单的处理
    try:
        deal_cancel(cur,cxdf)
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")        
        conn.rollback()
        cur.close()
        conn.close()        
        if len(cxdf)>0:
            myoutput(file+"有"+str(len(cxdf))+"条取消订单,请人工处理:")
            myoutput(cxdf)
            #send_mail("test程序",file+"有"+str(len(cxdf))+"条取消订单,请人工处理:"+str(cxdf))
        return False
    conn.commit()
    
    try:    
        #执行存储过程,将数据导入主表
        id = cur.var(cx_Oracle.NUMBER)
        cur.callproc("TGY_XINGYE_ALL_DATA_PROCESS",["SYSTEM",id])
        #检查是否有没有导入系统的数据
        cur.prepare("select count(*) num from tgy_xingye_import where 处理进订单=1 and DATA_IMPORT_PROCESS_ID=:id")
        cur.execute(None,{'id':id})
        #获取一条记录
        one = cur.fetchone()
        if one[0]>0:
            myoutput(file+"有"+str(one[0])+"条数据未导入系统,请进行检查")
            myoutput("")
            cur.close()
            conn.close()                
            return False       
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")        
        conn.rollback()
        cur.close()
        conn.close()    
        return False
        
    conn.commit()        
    cur.close()
    conn.close()
    return True

'''
理财卡的体检洁牙数据导入系统
'''    
def deal_lcktj(file):  
           
    myoutput("处理: "+file)    
    dataline = list(csv.reader(open(file, 'r')))
    '''
    f=open(file, 'r')   
    lines=f.readlines()
    f.close()
    #处理字符串格式转换
    
    for i in range(1,len(lines)):
        s = lines[i].strip()
        l = s.find('[')
        m = s.find(']',l)
        subs = s[l:m+1]
        subs = subs.replace(",",";")
        subs = subs.replace("'",'"') 
        #截取l之前+含[和]的字符串+]之后的字符串    
        line = s[:l]+subs+s[(m+1):]        
        dataline[i]=line.split(",") 
        
    print(dataline[0])    
    '''
    df=pd.DataFrame(dataline[1:],columns=dataline[0])
    
   # df = pd.read_csv(file,encoding='gb2312', dtype=str, keep_default_na=False) #gb2312只能简体字，有繁体字可能，报错
    cxdf = df[df["订单状态"].isin(["预约撤销","预约撤消"])].copy()
    cxdf = cxdf[["订单编号","订单状态"]]
    cxdf=cxdf.reset_index()
    df = df[~df["订单状态"].isin(["预约撤销","预约撤消"])].copy()    
    #过滤是否有团检,如果有生成csv文件为了后续导入
    df["Team"] = df["备注"].apply(lambda x:x.find("团检"))
    dfteam = df[df["Team"]>-1].copy()    
    #如果有团检生成团检文件
    if len(dfteam)>0:
        dfteam=dfteam.reset_index(drop=True)
        if create_team_file(dfteam,file) is False:
            return False
    
    df = df[df["Team"]<0].copy()
    df=df.reset_index()
    
    #df1 = pd.read_csv( path+f,',',encoding="gb2312",dtype=coltype) #直接读文件一直出现错误
	#理财卡 体检洁牙模板
    #sql = "insert into tgy_xingye_import(订单编号,客户身份,姓名,手机号,行权日期,订单状态,证件号码,服务项目,服务供应商,备注,本人次数,是否转让家人,使用人姓名,使用人证件号码,体检人性别,是否妇检,套餐)" 
    #sql=sql+" values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
	
    sql = "insert into tgy_xingye_import(订单编号,客户身份,姓名,手机号,行权日期,行权时间,是否租杆,订单状态,证件号码,服务项目,服务供应商,订单详情,备注)" 
    sql=sql+" values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    conn = test_orcle()
    cur = conn.cursor()
    if '客户身份' not in df.columns:
        df['客户身份'] = '黑金'
    if '行权时间' not in df.columns:
        df['行权时间'] = '000000'
    if '是否租杆' not in df.columns:
        df['是否租杆'] = 'N'
    try:
        cur.execute("alter session set nls_date_format='yyyymmdd'")
        for i,row in df.iterrows():
			#print("%s"%row[''])
            #print(i,len(df))
            idnotype=''
            if len(str(row["转让他人证件号码"]))==18 or len(str(row["转让他人证件号码"]))==15:
                idnotype='A'
            orderdetail='[{"本人":"'+str(row["本人次数"])+'"};{"性别(0:男1：女)":"'+str(row["性别"])+'"};{"转让他人":"'+str(row["转让他人"])+'"};{"转让他人姓名":"'+str(row["使用人姓名"])+'"};{"转让他人证件号码":"'+str(row["转让他人证件号码"])+'"};{"套餐":"'+str(row["套餐"])+'"};{"是否妇检":"'+str(row["是否妇检"])+'"};{"转让人证件类型":"'+idnotype+'"}]'
            cur.execute(sql%(row["订单编号"],row["客户身份"],row["姓名"],row["手机号"],row["行权日期"],row["行权时间"],row["是否租杆"],
				row["订单状态"],row["证件号码"],row["服务项目"]
				,row["服务供应商"],orderdetail,row["备注"]))
            
        #for i in range(len(df)):
        #    cur.execute(sql%(df.at[i,"订单编号"],df.at[i,"客户身份"],df.at[i,"姓名"],df.at[i,"手机号"],df.at[i,"行权日期"]
        #        ,df.at[i,"行权时间"],df.at[i,"是否租杆"],df.at[i,"订单状态"],df.at[i,"证件号码"],df.at[i,"服务项目"]
        #        ,df.at[i,"服务供应商"],df.at[i,"订单详情"],df.at[i,"备注"]))
        
        ##一次插入多条数据,参数为字典列表形式
        #param=[{'id':3,'n':'admin','p':'password'},{'id':4,'n':'admin','p':'password'},{'id':5,'n':'admin','p':'password'}];
        #cursor.executemany('insert into tb_user values(:id,:n,:p)',param);
        #保存文件导入记录
        savecompleted(cur,file)        
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        conn.rollback()
        cur.close()
        conn.close()
        return False        
    conn.commit()

    
    '''
    同一批文件中不会出现先有订单后又取消此订单的情况，所以取消订单先处理    
    '''
    #撤销订单的处理
    try:
        deal_cancel(cur,cxdf)
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")        
        conn.rollback()
        cur.close()
        conn.close()        
        if len(cxdf)>0:
            myoutput(file+"有"+str(len(cxdf))+"条取消订单,请人工处理:")
            myoutput(cxdf)
            #send_mail("test程序",file+"有"+str(len(cxdf))+"条取消订单,请人工处理:"+str(cxdf))
        return False
    conn.commit()
    
    try:    
        #执行存储过程,将数据导入主表
        id = cur.var(cx_Oracle.NUMBER)
        cur.callproc("TGY_XINGYE_ALL_DATA_PROCESS",["SYSTEM",id])
        #检查是否有没有导入系统的数据
        cur.prepare("select count(*) num from tgy_xingye_import where 处理进订单=1 and DATA_IMPORT_PROCESS_ID=:id")
        cur.execute(None,{'id':id})
        #获取一条记录
        one = cur.fetchone()
        if one[0]>0:
            myoutput(file+"有"+str(one[0])+"条数据未导入系统,请进行检查")
            myoutput("")
            cur.close()
            conn.close()                
            return False       
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")        
        conn.rollback()
        cur.close()
        conn.close()    
        return False
        
    conn.commit()        
    cur.close()
    conn.close()
    return True

'''
信用卡的数据导入系统
'''    
def deal_xyk(file):

    myoutput("处理: "+file)
    lines = list(csv.reader(open(file,'r')))
    #处理最后一列重复的备注
    if len(lines)>0 and lines[0][-1]==lines[0][-2]:
       lines[0][-1]="备注_1"
    #处理是否有医生的字段 
    if "意向医院、科室、医生" in lines[0]:
        i = lines[0].index("意向医院、科室、医生")
        lines[0][i]="意向医院科室医生"    
    
    df=pd.DataFrame(lines[1:],columns=lines[0])
    #如果没有这一列则要添加这一列
    if "意向医院科室医生" not in lines[0]:
        df["意向医院科室医生"]=""
    if "体检版本" not in lines[0]:
        df["体检版本"]=""   
    if "体检人性别" not in lines[0]:
        df["体检人性别"]=""    
    cxdf = df[df["订单状态"].isin(["预约撤销","预约撤消"])].copy()
    cxdf = cxdf[["订单编号","订单状态"]]
    cxdf = cxdf.reset_index()
    df = df[~df["订单状态"].isin(["预约撤销","预约撤消"])].copy()
    df = df.reset_index()
    #df1 = pd.read_csv( path+f,',',encoding="gb2312",dtype=coltype) #直接读文件一直出现错误
    sql = "insert into tgy_xingye_import(订单编号,订单状态,姓名,手机号,服务类型,服务项目,行权日期,行权时间,本人次数,是否转让家人,本人证件号码,使用人姓名,使用人联系电话,使用人证件号码,意向医院科室医生,备注,体检版本,体检人性别)" 
    sql=sql+" values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    conn = test_orcle()
    cur = conn.cursor()
    try:
        cur.execute("alter session set nls_date_format='yyyymmdd'")    
        for i,row in df.iterrows():
            cur.execute(sql%(row["订单编号"],row["订单状态"]
                ,row["姓名"],row["手机号"],row["服务类型"],row["服务项目"]
                ,row["行权日期"],row["行权时间"]
                ,row["本人次数"],row["是否转让家人"]
                ,row["本人证件号码"],row["使用人姓名"],row["使用人联系电话"]
                ,row["使用人证件号码"],row["意向医院科室医生"],row["备注"],row['体检版本'],row['体检人性别']))
        
        #param={'id':2,'n':'admin','p':'password'}
        #cursor.execute('insert into tb_user values(:id,:n,:p)',param);
        ##一次插入多条数据,参数为字典列表形式
        #param=[{'id':3,'n':'admin','p':'password'},{'id':4,'n':'admin','p':'password'},{'id':5,'n':'admin','p':'password'}];
        #cursor.executemany('insert into tb_user values(:id,:n,:p)',param);    
        #保存文件导入记录
        savecompleted(cur,file)
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        conn.rollback()
        cur.close()
        conn.close()
        return False
    conn.commit()
    
    #撤销订单的处理
    try:
        deal_cancel(cur,cxdf)
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")        
        conn.rollback()
        cur.close()
        conn.close()        
        if len(cxdf)>0:
            myoutput(file+"有"+str(len(cxdf))+"条取消订单,请人工处理:")
            myoutput(cxdf)
            #send_mail("test程序",file+"有"+str(len(cxdf))+"条取消订单,请人工处理:"+str(cxdf))
        return False     
    conn.commit()
    
    try:
        #执行存储过程,将数据导入主表
        id = cur.var(cx_Oracle.NUMBER)
        cur.callproc("TGY_XINGYE_ALL_DATA_PROCESS",["SYSTEM",id])
        #检查是否有没有导入系统的数据
        cur.prepare("select count(*) num from tgy_xingye_import where 处理进订单=1 and DATA_IMPORT_PROCESS_ID=:id")
        cur.execute(None,{'id':id})
        #获取一条记录
        one = cur.fetchone()
        if one[0]>0 :
            myoutput(file+"有"+str(one[0])+"条数据未导入系统,请进行检查")
            myoutput("")        
            #send_mail("test程序",file+"有"+str(one[0])+"条数据未导入系统,请进行检查")
            cur.close()
            conn.close()
            return False    
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")        
        conn.rollback()
        cur.close()
        conn.close()
        return False    
        
    conn.commit()        
    cur.close()
    conn.close()
    return True

'''
私行的数据导入系统
'''    
def deal_sh(file):           
    myoutput("处理: "+file)
    lines = list(csv.reader(open(file,'r')))
    #处理最后一列重复的备注
    if len(lines)>0 and lines[0][-1]==lines[0][-2]:
       lines[0][-1]="备注_1"
    #处理是否有医生的字段 
    if "就医意向（意向医院|科室|医生）" in lines[0]:
        i = lines[0].index("就医意向（意向医院|科室|医生）")
        lines[0][i]="意向医院科室医生"    
    
    df=pd.DataFrame(lines[1:],columns=lines[0])
    #如果没有这一列则要添加这一列
    if "意向医院科室医生" not in lines[0]:
        df["意向医院科室医生"]=""
    #添加客户身份一列
    df["客户身份"]="兴业私人银行专家挂号卡"
    cxdf = df[df["订单状态"].isin(["预约撤销","预约撤消"])].copy()
    cxdf = cxdf[["订单编号","订单状态"]]
    cxdf = cxdf.reset_index()    
    df = df[~df["订单状态"].isin(["预约撤销","预约撤消"])].copy()
    df = df.reset_index()   
            
    #df1 = pd.read_csv( path+f,',',encoding="gb2312",dtype=coltype) #直接读文件一直出现错误
    sql = "insert into tgy_xingye_import(订单编号,订单状态,姓名,手机号,证件号码,服务项目,行权日期,本人次数,服务供应商,意向医院科室医生,备注,客户身份)" 
    sql=sql+" values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    conn = test_orcle()
    cur = conn.cursor()
    
    try:
        cur.execute("alter session set nls_date_format='yyyymmdd'")
        for i,row in df.iterrows():
            cur.execute(sql%(row["订单编号"],row["订单状态"]
                ,row["姓名"],row["手机号"],row["证件号码"],row["服务项目"]
                ,row["行权日期"],row["本人次数"]
                ,row["服务供应商"]
                ,row["意向医院科室医生"],row["备注"],row["客户身份"]))
        
        #param={'id':2,'n':'admin','p':'password'}
        #cursor.execute('insert into tb_user values(:id,:n,:p)',param);
        ##一次插入多条数据,参数为字典列表形式
        #param=[{'id':3,'n':'admin','p':'password'},{'id':4,'n':'admin','p':'password'},{'id':5,'n':'admin','p':'password'}];
        #cursor.executemany('insert into tb_user values(:id,:n,:p)',param);    
        #保存文件导入记录
        savecompleted(cur,file)
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        conn.rollback()
        cur.close()
        conn.close()
        return False
    conn.commit()
    
    #撤销订单的处理
    try:
        deal_cancel(cur,cxdf)
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")        
        conn.rollback()
        cur.close()
        conn.close()        
        if len(cxdf)>0:
            myoutput(file+"有"+str(len(cxdf))+"条取消订单,请人工处理:")
            myoutput(cxdf)
            #send_mail("test程序",file+"有"+str(len(cxdf))+"条取消订单,请人工处理:"+str(cxdf))
        return False 
    conn.commit()
    
    try:
        #执行存储过程,将数据导入主表
        id = cur.var(cx_Oracle.NUMBER)
        cur.callproc("TGY_XINGYE_ALL_DATA_PROCESS",["SYSTEM",id])
        #检查是否有没有导入系统的数据
        cur.prepare("select count(*) num from tgy_xingye_import where 处理进订单=1 and DATA_IMPORT_PROCESS_ID=:id")
        cur.execute(None,{'id':id})
        #获取一条记录
        one = cur.fetchone()
        if one[0]>0 :
            myoutput(file+"有"+str(one[0])+"条数据未导入系统,请进行检查")
            myoutput("")        
            #send_mail("test程序",file+"有"+str(one[0])+"条数据未导入系统,请进行检查")
            cur.close()
            conn.close()
            return False
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        conn.rollback()
        cur.close()
        conn.close()
        return False
    
    conn.commit()        
    cur.close()
    conn.close()
    return True
    
'''
团检的数据导入系统
'''    
def deal_team(file):           
    myoutput("处理: "+file)
    lines = list(csv.reader(open(file,'r')))
    #处理最后一列重复的备注
    if len(lines)>0 and lines[0][-1]==lines[0][-2]:
       lines[0][-1]="备注_1"   
    
    df=pd.DataFrame(lines[1:],columns=lines[0])
    #如果没有这一列则要添加这一列
    if "意向医院科室医生" not in lines[0]:
        df["意向医院科室医生"]=""    
    df = df.reset_index()
    
    #df1 = pd.read_csv( path+f,',',encoding="gb2312",dtype=coltype) #直接读文件一直出现错误
    sql = "insert into tgy_xingye_team_import(订单编号,姓名,手机号,证件号码,服务项目,行权日期,备注,客户身份,订单详情)" 
    sql=sql+" values('%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    conn = test_orcle()
    cur = conn.cursor()
    
    try:
        cur.execute("alter session set nls_date_format='yyyymmdd'")
        for i,row in df.iterrows():
            cur.execute(sql%(row["订单编号"]
                ,row["姓名"],row["手机号"],row["证件号码"],row["服务项目"]
                ,row["行权日期"],row["备注"],row["客户身份"],row["订单详情"]))
        
        #param={'id':2,'n':'admin','p':'password'}
        #cursor.execute('insert into tb_user values(:id,:n,:p)',param);
        ##一次插入多条数据,参数为字典列表形式
        #param=[{'id':3,'n':'admin','p':'password'},{'id':4,'n':'admin','p':'password'},{'id':5,'n':'admin','p':'password'}];
        #cursor.executemany('insert into tb_user values(:id,:n,:p)',param);    
        #保存文件导入记录
        savecompleted_team(cur,file)
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        conn.rollback()
        cur.close()
        conn.close()
        return False
    conn.commit()
    
    try:
        #执行存储过程,将数据导入主表
        id = cur.var(cx_Oracle.NUMBER)
        cur.callproc("TGY_XINGYE_TEAM_PROCESS",["SYSTEM",id])
        #检查是否有没有导入系统的数据
        cur.prepare("select count(*) num from tgy_xingye_team_import where 处理进订单=1 and DATA_IMPORT_PROCESS_ID=:id")
        cur.execute(None,{'id':id})
        #获取一条记录
        one = cur.fetchone()
        if one[0]>0 :
            myoutput(file+"有"+str(one[0])+"条数据未导入系统,请进行检查")
            myoutput("")        
            #send_mail("test程序",file+"有"+str(one[0])+"条数据未导入系统,请进行检查")
            cur.close()
            conn.close()
            return False    
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        conn.rollback()
        cur.close()
        conn.close()
        return False
        
    conn.commit()        
    cur.close()
    conn.close()
    return True    
   
'''
首先看看数据是否能正常分析,没有分析处理的数据需要即时处理掉
'''   
def first_process():
    
    try:
        conn = test_orcle()
        cur = conn.cursor()
        #执行存储过程,将数据导入主表
        id = cur.var(cx_Oracle.NUMBER)
        cur.callproc("TGY_XINGYE_ALL_DATA_PROCESS",["SYSTEM",id])
        #检查是否有没有导入系统的数据
        cur.prepare("select count(*) num from tgy_xingye_import where 处理进订单=1 and DATA_IMPORT_PROCESS_ID=:id")
        cur.execute(None,{'id':id})
        #获取一条记录
        one = cur.fetchone()
        if one[0]>0 :
            myoutput(file+"有"+str(one[0])+"条数据未导入系统,请进行检查")
            myoutput("")        
            #send_mail("test程序",file+"有"+str(one[0])+"条数据未导入系统,请进行检查")
            cur.close()
            conn.close()            
            return False
    except Exception as e:
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        myoutput(e) 
        myoutput("\033[1;31;47m!!!!!!!!!!!!!!!! ERROR !!!!!!!!!!!!!!!! ")
        conn.rollback()
        cur.close()
        conn.close()
        return False
    
    conn.commit()        
    cur.close()
    conn.close()

    
if __name__ == "__main__":
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    today = time.strftime("%Y%m%d", time.localtime())
    
    logging.basicConfig(level=logging.INFO,
                    filename='C:/cib/log/'+today+".log",
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(asctime)s - %(message)s')
                    
    myoutput(now)
    checkpath()  
        
    #首先检查数据分析正常，防止部分文件不正常影响后续订单
    #有问题的即时处理
    rst = first_process()
    if rst is False:
        myoutput("============================")
        myoutput("\033[1;31;47m!!!!!!请处理后再进行!!!!!!!!")
        myoutput("============================")
        sys.exit(0)   #此处为强制退出处理，有错误必须修正才继续。但后面的注释是为了保证绝大部分单子的处理。
    
    #理财卡挂号就医
       
    lck_guahao_jiuyi = cib["lck_guahao_jiuyi"]+today+"/"
    flist = get_todeal_list(lck_guahao_jiuyi)
    for f in flist:
        rst = deal_lck(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!请处理后再进行!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)
        else:
            myoutput("OK")    
             
    #理财卡体检洁牙       
    lck_tijian_jieya = cib["lck_tijian_jieya"]+today+"/"
    flist = get_todeal_list(lck_tijian_jieya)
    for f in flist:
        rst = deal_lcktj(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!请处理后再进行!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)
        else:
            myoutput("OK")        
    
    #信用卡挂号预约       
    xyk_guahao = cib["xyk_guahao"]+today+"/"
    flist = get_todeal_list(xyk_guahao)
    for f in flist:
        rst = deal_xyk(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!请处理后再进行!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)
        else:
            myoutput("OK")
    
    #信用卡就医       
    xyk_jiuyi = cib["xyk_jiuyi"]+today+"/"
    flist = get_todeal_list(xyk_jiuyi)
    for f in flist:
        rst = deal_xyk(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!请处理后再进行!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)
        else:
            myoutput("OK")                
    
    #信用卡体检       
    xyk_tianjian = cib["xyk_tianjian"]+today+"/"
    flist = get_todeal_list(xyk_tianjian)
    for f in flist:
        rst = deal_xyk(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!请处理后再进行!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)
        else:
            myoutput("OK")    
    
    #信用卡洁牙       
    xyk_jieya = cib["xyk_jieya"]+today+"/"
    flist = get_todeal_list(xyk_jieya)
    for f in flist:
        rst = deal_xyk(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!请处理后再进行!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)  
        else:
            myoutput("OK")   

    #私行就医    
    sh_guibin_jiuyi = cib["sh_guibin_jiuyi"]+today+"/"
    flist = get_todeal_list(sh_guibin_jiuyi)
    for f in flist:
        rst = deal_sh(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!请处理后再进行!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)  
        else:
            myoutput("OK")        

    #团检订单
    lck_team = cib["lck_team"]+today+"/"
    flist = get_todeal_list_team(lck_team)
    for f in flist:
        rst = deal_team(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!请处理后再进行!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)  
        else:
            myoutput("OK")  
           
    myoutput("---(::)---")        
    myoutput("---(::)---")
    myoutput("---(::)---")

    #自动补齐,对满足要求的订单自动进行补齐,不需要人工干预
    conn = test_orcle()
    cur = conn.cursor()
    cur.execute("alter session set nls_date_format='yyyymmdd'")
    v_num = cur.var(cx_Oracle.NUMBER)
    cur.callproc("TGY_XINGYE_ZERO_ORDER_DEAL",["SYSTEM",v_num])
    if v_num.getvalue()>0:
        myoutput("自动补齐订单数量:"+str(v_num.getvalue()))   
    print("------注意检查红字提醒-------")
    sys.exit(0)
    
    