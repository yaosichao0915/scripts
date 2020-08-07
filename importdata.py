# -*- coding: gb2312 -*-
#python -m py_compile "d:\cib\importdata.py"

'''
��������һ��ʱ�䣬�����������⣺
1�����ص��ļ����ֶζ࣬��ע��������ж��� ","�ţ������ж࣬��ʱ�޸��ļ�����
2�������ظ�����������ɾ���ظ����ļ�����
3���ڽ����ݵ�����ʽ��ʱ���ִ��󣬽��û����־���޸� 2019/10/30

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
"lck_guahao_jiuyi":"D:/cib/��ƿ�/ԤԼ�Һ�/",
"lck_tijian_jieya":"D:/cib/��ƿ�/������/",
"xyk_jiuyi":"D:/cib/���ÿ�/���Դ-�����ҽ/",
"xyk_jieya":"D:/cib/���ÿ�/���Դ-����/",
"xyk_tianjian":"D:/cib/���ÿ�/���Դ-���/",
"xyk_guahao":"D:/cib/���ÿ�/���Դ-ר��ԤԼ/",
"sh_guibin_jiuyi":"D:/cib/˽��/�����ҽ/",
"lck_team":"D:/cib/�ż�/",
}

def myoutput(msg):
    logger = logging.getLogger("importdata") 
    logger.info(msg)
    print(msg)

#�����ʼ�    
def send_mail(subject,content,tolist=None):
    smtpserver = 'smtp.exmail.qq.com'  
    smtpport = 465
    username = 'log.sys@tianguyuan.com'  
    password = 'Tian2017#'
    sender = 'log.sys@tianguyuan.com'  
    if tolist == None :
        tolist = ["jialai.hao@tianguyuan.com","pingfan.zhu@tianguyuan.com","xia.chen@tianguyuan.com","liulei.lv@tgene.com.cn"]

    try:
        msg = MIMEText(content,'html','utf-8')#�����������utf-8�������ֽ��ַ�����Ҫ  
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
oracle����
'''
def test_orcle():
    host = "192.168.1.247"   #���ݿ�ip
    port = "1521"        #�˿�
    sid = "orcl"         #���ݿ�����
    dsn = cx_Oracle.makedsn(host, port, sid)
    conn = cx_Oracle.connect("tgycrm", "Tianguyuan123#", dsn)
    #conn = cx_Oracle.connect("tgycrmdev", "Tianguyuan123#", dsn)
    sql = 'select sysdate from dual'
    rst = pd.read_sql(sql,conn)
    return conn

'''
���Ŀ¼�ļ��Ƿ����,����������Ҫ����
'''    
def checkpath(): 
    list = cib.keys();
    for key in cib.keys():
        if os.path.exists(cib[key]):
            myoutput(cib[key]+" Ŀ¼����")
        else:
            os.makedirs(cib[key])
            if os.path.exists(cib[key]):
                myoutput(cib[key]+" Ŀ¼����")
            else:
                myoutput(cib[key]+" Ŀ¼����ʧ��")
                return False

'''
��ȡĿ¼�µ�����.csv���ļ�,���Ŀ¼û���Զ�����
'''
def get_filelist(path):
    list = None
    if not os.path.exists(path):
        os.makedirs(path)
        if not os.path.exists(path):
            myoutput(cib[key]+" Ŀ¼����ʧ��")
            return list
            
    list = os.listdir(path)
    return list
    
    #if not os.path.exists(path):
    #    myoutput(path+" ��Ŀ¼")
    #    return list
    #else:
    #    list = os.listdir(path)
    #    return list

'''
����ļ��Ƿ��Ѿ�����,�������������Ҫ����
'''
def checkcompleted(cursor,file):
    cursor.prepare("select count(*) num from tgy_xingye_import_his where filepath=:filepath")
    cursor.execute(None,{'filepath':file})
    #��ȡһ����¼
    one = cursor.fetchone()
    if one[0]>0 :        
        return True
    return False

def checkcompleted_team(cursor,file):
    cursor.prepare("select count(*) num from tgy_xingye_team_import_his where filepath=:filepath")
    cursor.execute(None,{'filepath':file})
    #��ȡһ����¼
    one = cursor.fetchone()
    if one[0]>0 :        
        return True
    return False    

'''
���洦����ɵ��ļ�,һ��δ·��+file�ķ�ʽ
�洢�˱����ʱ���
'''
def savecompleted(cursor,file):
    cursor.execute("insert into tgy_xingye_import_his(filepath) values(:filepath)",{"filepath":file})
    return True
    
def savecompleted_team(cursor,file):
    cursor.execute("insert into tgy_xingye_team_import_his(filepath) values(:filepath)",{"filepath":file})
    return True    
    
'''
��ȡ��Ҫ������ļ��б�
'''
def get_todeal_list(path):
    l = []
    flist = get_filelist(path)
    #���Ŀ¼��������ֱ�ӷ���
    if flist is None:
        return l
    
    #���û���ļ��򲻽��д���
    if len(flist)<1 :
        myoutput(path+" ���ļ���Ҫ����")
        return l
   
    conn = test_orcle()
    cur = conn.cursor()
    for f in flist:
        #����Ƿ��Ѿ�����,����Ѿ���������Ҫ����
        if checkcompleted(cur,path+f):
            myoutput(path+f+" �Ѿ�����")
            continue
        l.append(path+f)
    cur.close()
    conn.close()
    
    #����֤�ļ�������˳����
    l.sort()
    return l
    
def get_todeal_list_team(path):
    l = []
    flist = get_filelist(path)
    #���Ŀ¼��������ֱ�ӷ���
    if flist is None:
        return l
    
    #���û���ļ��򲻽��д���
    if len(flist)<1 :
        myoutput(path+" ���ļ���Ҫ����")
        return l
    
    conn = test_orcle()
    cur = conn.cursor()
    for f in flist:
        #����Ƿ��Ѿ�����,����Ѿ���������Ҫ����
        if checkcompleted_team(cur,path+f):
            myoutput(path+f+" �Ѿ�����")
            continue
        l.append(path+f)
    cur.close()
    conn.close()
    #����֤�ļ�������˳����
    l.sort()
    return l    
 
'''
������������,�����������ɵ��ù��̹��������Ƿ�ͬʱ���
���ڶ��γ����˶���ʱ�������Ѿ�����������Ϊ�ɹ�
cursor Ϊ�α�
����ǵ���״̬�Ķ���ȡ����ʲô�����?
���û�е���
'''
def deal_cancel(cursor,df):
    if df is None or len(df)<1:
        return True  
    #���ݶ�����ȡ��������ȡ��ʱ�б�ע
    for i,row in df.iterrows():
        bank_no = row["�������"]
        #���д洢����ȡ������
        #ִ�д洢����,�����ݵ�������
        ord_id = cursor.var(cx_Oracle.NUMBER)
        cursor.callproc("TGY_XINGYE_ALL_DATA_CANCEL",["SYSTEM",bank_no,ord_id])        
        #���������,����False
    return True  
    
'''
�����ż쵥�ļ�,���ø��ǵķ�ʽ
'''
def create_team_file(df,filepath):    
    if len(df)<1:
        return True
        
    path = cib["lck_team"]+time.strftime("%Y%m%d", time.localtime())+"/"
    if os.path.exists(path) is False:            
        os.makedirs(path)
        if os.path.exists(path) is False:
            myoutput(cib[key]+" Ŀ¼����ʧ��")
            return False
    filename = path+os.path.basename(filepath)
    rt = df.to_csv(filename,index=False,encoding="gbk") 
    #print("---------team team team ---------")
    #print("���ż��ļ���������ż��ļ��������"+filename)
    #print("---------team team team ---------")
    return True
    
    
'''
��ƿ���ԤԼ�Һ����ݵ���ϵͳ
��ƿ���ԤԼ�Һ����ݵĶ�������������['':'','':'']��ʽ,��Ҫ����Ϊ["":"";"":""]��ʽ
'''    
def deal_lck(file):  
           
    myoutput("����: "+file)    
    dataline = list(csv.reader(open(file, 'r')))
    f=open(file, 'r')   
    lines=f.readlines()
    f.close()
    #�����ַ�����ʽת��
    for i in range(1,len(lines)):
        s = lines[i].strip()
        l = s.find('[')
        m = s.find(']',l)
        subs = s[l:m+1]
        subs = subs.replace(",",";")
        subs = subs.replace("'",'"') 
        #��ȡl֮ǰ+��[��]���ַ���+]֮����ַ���    
        line = s[:l]+subs+s[(m+1):]        
        dataline[i]=line.split(",") 
        
    df=pd.DataFrame(dataline[1:],columns=dataline[0])
    cxdf = df[df["����״̬"].isin(["ԤԼ����","ԤԼ����"])].copy()
    cxdf = cxdf[["�������","����״̬"]]
    cxdf=cxdf.reset_index()
    df = df[~df["����״̬"].isin(["ԤԼ����","ԤԼ����"])].copy()    
    #�����Ƿ����ż�,���������csv�ļ�Ϊ�˺�������
    df["Team"] = df["��ע"].apply(lambda x:x.find("�ż�"))
    dfteam = df[df["Team"]>-1].copy()    
    #������ż������ż��ļ�
    if len(dfteam)>0:
        dfteam=dfteam.reset_index(drop=True)
        if create_team_file(dfteam,file) is False:
            return False
    
    df = df[df["Team"]<0].copy()
    df=df.reset_index()
    
    #df1 = pd.read_csv( path+f,',',encoding="gb2312",dtype=coltype) #ֱ�Ӷ��ļ�һֱ���ִ���
	#��ƿ� ԤԼ�Һ�ģ��
    sql = "insert into tgy_xingye_import(�������,�ͻ����,����,�ֻ���,��Ȩ����,��Ȩʱ��,�Ƿ����,����״̬,֤������,������Ŀ,����Ӧ��,��������,��ע)" 
    sql=sql+" values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
	
    conn = test_orcle()
    cur = conn.cursor()
    
    try:
        cur.execute("alter session set nls_date_format='yyyymmdd'")
        for i,row in df.iterrows():
            #print(i,len(df))
            cur.execute(sql%(row["�������"],row["�ͻ����"],row["����"],row["�ֻ���"],row["��Ȩ����"]
					,row["��Ȩʱ��"],row["�Ƿ����"],row["����״̬"],row["֤������"],row["������Ŀ"]
					,row["����Ӧ��"],row["��������"],row["��ע"]))
            
        #for i in range(len(df)):
        #    cur.execute(sql%(df.at[i,"�������"],df.at[i,"�ͻ����"],df.at[i,"����"],df.at[i,"�ֻ���"],df.at[i,"��Ȩ����"]
        #        ,df.at[i,"��Ȩʱ��"],df.at[i,"�Ƿ����"],df.at[i,"����״̬"],df.at[i,"֤������"],df.at[i,"������Ŀ"]
        #        ,df.at[i,"����Ӧ��"],df.at[i,"��������"],df.at[i,"��ע"]))
        
        ##һ�β����������,����Ϊ�ֵ��б���ʽ
        #param=[{'id':3,'n':'admin','p':'password'},{'id':4,'n':'admin','p':'password'},{'id':5,'n':'admin','p':'password'}];
        #cursor.executemany('insert into tb_user values(:id,:n,:p)',param);
        #�����ļ������¼
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
    ͬһ���ļ��в���������ж�������ȡ���˶��������������ȡ�������ȴ���    
    '''
    #���������Ĵ���
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
            myoutput(file+"��"+str(len(cxdf))+"��ȡ������,���˹�����:")
            myoutput(cxdf)
            #send_mail("test����",file+"��"+str(len(cxdf))+"��ȡ������,���˹�����:"+str(cxdf))
        return False
    conn.commit()
    
    try:    
        #ִ�д洢����,�����ݵ�������
        id = cur.var(cx_Oracle.NUMBER)
        cur.callproc("TGY_XINGYE_ALL_DATA_PROCESS",["SYSTEM",id])
        #����Ƿ���û�е���ϵͳ������
        cur.prepare("select count(*) num from tgy_xingye_import where ���������=1 and DATA_IMPORT_PROCESS_ID=:id")
        cur.execute(None,{'id':id})
        #��ȡһ����¼
        one = cur.fetchone()
        if one[0]>0:
            myoutput(file+"��"+str(one[0])+"������δ����ϵͳ,����м��")
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
��ƿ������������ݵ���ϵͳ
'''    
def deal_lcktj(file):  
           
    myoutput("����: "+file)    
    dataline = list(csv.reader(open(file, 'r')))
    '''
    f=open(file, 'r')   
    lines=f.readlines()
    f.close()
    #�����ַ�����ʽת��
    
    for i in range(1,len(lines)):
        s = lines[i].strip()
        l = s.find('[')
        m = s.find(']',l)
        subs = s[l:m+1]
        subs = subs.replace(",",";")
        subs = subs.replace("'",'"') 
        #��ȡl֮ǰ+��[��]���ַ���+]֮����ַ���    
        line = s[:l]+subs+s[(m+1):]        
        dataline[i]=line.split(",") 
        
    print(dataline[0])    
    '''
    df=pd.DataFrame(dataline[1:],columns=dataline[0])
    
   # df = pd.read_csv(file,encoding='gb2312', dtype=str, keep_default_na=False) #gb2312ֻ�ܼ����֣��з����ֿ��ܣ�����
    cxdf = df[df["����״̬"].isin(["ԤԼ����","ԤԼ����"])].copy()
    cxdf = cxdf[["�������","����״̬"]]
    cxdf=cxdf.reset_index()
    df = df[~df["����״̬"].isin(["ԤԼ����","ԤԼ����"])].copy()    
    #�����Ƿ����ż�,���������csv�ļ�Ϊ�˺�������
    df["Team"] = df["��ע"].apply(lambda x:x.find("�ż�"))
    dfteam = df[df["Team"]>-1].copy()    
    #������ż������ż��ļ�
    if len(dfteam)>0:
        dfteam=dfteam.reset_index(drop=True)
        if create_team_file(dfteam,file) is False:
            return False
    
    df = df[df["Team"]<0].copy()
    df=df.reset_index()
    
    #df1 = pd.read_csv( path+f,',',encoding="gb2312",dtype=coltype) #ֱ�Ӷ��ļ�һֱ���ִ���
	#��ƿ� ������ģ��
    #sql = "insert into tgy_xingye_import(�������,�ͻ����,����,�ֻ���,��Ȩ����,����״̬,֤������,������Ŀ,����Ӧ��,��ע,���˴���,�Ƿ�ת�ü���,ʹ��������,ʹ����֤������,������Ա�,�Ƿ񸾼�,�ײ�)" 
    #sql=sql+" values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
	
    sql = "insert into tgy_xingye_import(�������,�ͻ����,����,�ֻ���,��Ȩ����,��Ȩʱ��,�Ƿ����,����״̬,֤������,������Ŀ,����Ӧ��,��������,��ע)" 
    sql=sql+" values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    conn = test_orcle()
    cur = conn.cursor()
    if '�ͻ����' not in df.columns:
        df['�ͻ����'] = '�ڽ�'
    if '��Ȩʱ��' not in df.columns:
        df['��Ȩʱ��'] = '000000'
    if '�Ƿ����' not in df.columns:
        df['�Ƿ����'] = 'N'
    try:
        cur.execute("alter session set nls_date_format='yyyymmdd'")
        for i,row in df.iterrows():
			#print("%s"%row[''])
            #print(i,len(df))
            idnotype=''
            if len(str(row["ת������֤������"]))==18 or len(str(row["ת������֤������"]))==15:
                idnotype='A'
            orderdetail='[{"����":"'+str(row["���˴���"])+'"};{"�Ա�(0:��1��Ů)":"'+str(row["�Ա�"])+'"};{"ת������":"'+str(row["ת������"])+'"};{"ת����������":"'+str(row["ʹ��������"])+'"};{"ת������֤������":"'+str(row["ת������֤������"])+'"};{"�ײ�":"'+str(row["�ײ�"])+'"};{"�Ƿ񸾼�":"'+str(row["�Ƿ񸾼�"])+'"};{"ת����֤������":"'+idnotype+'"}]'
            cur.execute(sql%(row["�������"],row["�ͻ����"],row["����"],row["�ֻ���"],row["��Ȩ����"],row["��Ȩʱ��"],row["�Ƿ����"],
				row["����״̬"],row["֤������"],row["������Ŀ"]
				,row["����Ӧ��"],orderdetail,row["��ע"]))
            
        #for i in range(len(df)):
        #    cur.execute(sql%(df.at[i,"�������"],df.at[i,"�ͻ����"],df.at[i,"����"],df.at[i,"�ֻ���"],df.at[i,"��Ȩ����"]
        #        ,df.at[i,"��Ȩʱ��"],df.at[i,"�Ƿ����"],df.at[i,"����״̬"],df.at[i,"֤������"],df.at[i,"������Ŀ"]
        #        ,df.at[i,"����Ӧ��"],df.at[i,"��������"],df.at[i,"��ע"]))
        
        ##һ�β����������,����Ϊ�ֵ��б���ʽ
        #param=[{'id':3,'n':'admin','p':'password'},{'id':4,'n':'admin','p':'password'},{'id':5,'n':'admin','p':'password'}];
        #cursor.executemany('insert into tb_user values(:id,:n,:p)',param);
        #�����ļ������¼
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
    ͬһ���ļ��в���������ж�������ȡ���˶��������������ȡ�������ȴ���    
    '''
    #���������Ĵ���
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
            myoutput(file+"��"+str(len(cxdf))+"��ȡ������,���˹�����:")
            myoutput(cxdf)
            #send_mail("test����",file+"��"+str(len(cxdf))+"��ȡ������,���˹�����:"+str(cxdf))
        return False
    conn.commit()
    
    try:    
        #ִ�д洢����,�����ݵ�������
        id = cur.var(cx_Oracle.NUMBER)
        cur.callproc("TGY_XINGYE_ALL_DATA_PROCESS",["SYSTEM",id])
        #����Ƿ���û�е���ϵͳ������
        cur.prepare("select count(*) num from tgy_xingye_import where ���������=1 and DATA_IMPORT_PROCESS_ID=:id")
        cur.execute(None,{'id':id})
        #��ȡһ����¼
        one = cur.fetchone()
        if one[0]>0:
            myoutput(file+"��"+str(one[0])+"������δ����ϵͳ,����м��")
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
���ÿ������ݵ���ϵͳ
'''    
def deal_xyk(file):

    myoutput("����: "+file)
    lines = list(csv.reader(open(file,'r')))
    #�������һ���ظ��ı�ע
    if len(lines)>0 and lines[0][-1]==lines[0][-2]:
       lines[0][-1]="��ע_1"
    #�����Ƿ���ҽ�����ֶ� 
    if "����ҽԺ�����ҡ�ҽ��" in lines[0]:
        i = lines[0].index("����ҽԺ�����ҡ�ҽ��")
        lines[0][i]="����ҽԺ����ҽ��"    
    
    df=pd.DataFrame(lines[1:],columns=lines[0])
    #���û����һ����Ҫ�����һ��
    if "����ҽԺ����ҽ��" not in lines[0]:
        df["����ҽԺ����ҽ��"]=""
    if "���汾" not in lines[0]:
        df["���汾"]=""   
    if "������Ա�" not in lines[0]:
        df["������Ա�"]=""    
    cxdf = df[df["����״̬"].isin(["ԤԼ����","ԤԼ����"])].copy()
    cxdf = cxdf[["�������","����״̬"]]
    cxdf = cxdf.reset_index()
    df = df[~df["����״̬"].isin(["ԤԼ����","ԤԼ����"])].copy()
    df = df.reset_index()
    #df1 = pd.read_csv( path+f,',',encoding="gb2312",dtype=coltype) #ֱ�Ӷ��ļ�һֱ���ִ���
    sql = "insert into tgy_xingye_import(�������,����״̬,����,�ֻ���,��������,������Ŀ,��Ȩ����,��Ȩʱ��,���˴���,�Ƿ�ת�ü���,����֤������,ʹ��������,ʹ������ϵ�绰,ʹ����֤������,����ҽԺ����ҽ��,��ע,���汾,������Ա�)" 
    sql=sql+" values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    conn = test_orcle()
    cur = conn.cursor()
    try:
        cur.execute("alter session set nls_date_format='yyyymmdd'")    
        for i,row in df.iterrows():
            cur.execute(sql%(row["�������"],row["����״̬"]
                ,row["����"],row["�ֻ���"],row["��������"],row["������Ŀ"]
                ,row["��Ȩ����"],row["��Ȩʱ��"]
                ,row["���˴���"],row["�Ƿ�ת�ü���"]
                ,row["����֤������"],row["ʹ��������"],row["ʹ������ϵ�绰"]
                ,row["ʹ����֤������"],row["����ҽԺ����ҽ��"],row["��ע"],row['���汾'],row['������Ա�']))
        
        #param={'id':2,'n':'admin','p':'password'}
        #cursor.execute('insert into tb_user values(:id,:n,:p)',param);
        ##һ�β����������,����Ϊ�ֵ��б���ʽ
        #param=[{'id':3,'n':'admin','p':'password'},{'id':4,'n':'admin','p':'password'},{'id':5,'n':'admin','p':'password'}];
        #cursor.executemany('insert into tb_user values(:id,:n,:p)',param);    
        #�����ļ������¼
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
    
    #���������Ĵ���
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
            myoutput(file+"��"+str(len(cxdf))+"��ȡ������,���˹�����:")
            myoutput(cxdf)
            #send_mail("test����",file+"��"+str(len(cxdf))+"��ȡ������,���˹�����:"+str(cxdf))
        return False     
    conn.commit()
    
    try:
        #ִ�д洢����,�����ݵ�������
        id = cur.var(cx_Oracle.NUMBER)
        cur.callproc("TGY_XINGYE_ALL_DATA_PROCESS",["SYSTEM",id])
        #����Ƿ���û�е���ϵͳ������
        cur.prepare("select count(*) num from tgy_xingye_import where ���������=1 and DATA_IMPORT_PROCESS_ID=:id")
        cur.execute(None,{'id':id})
        #��ȡһ����¼
        one = cur.fetchone()
        if one[0]>0 :
            myoutput(file+"��"+str(one[0])+"������δ����ϵͳ,����м��")
            myoutput("")        
            #send_mail("test����",file+"��"+str(one[0])+"������δ����ϵͳ,����м��")
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
˽�е����ݵ���ϵͳ
'''    
def deal_sh(file):           
    myoutput("����: "+file)
    lines = list(csv.reader(open(file,'r')))
    #�������һ���ظ��ı�ע
    if len(lines)>0 and lines[0][-1]==lines[0][-2]:
       lines[0][-1]="��ע_1"
    #�����Ƿ���ҽ�����ֶ� 
    if "��ҽ��������ҽԺ|����|ҽ����" in lines[0]:
        i = lines[0].index("��ҽ��������ҽԺ|����|ҽ����")
        lines[0][i]="����ҽԺ����ҽ��"    
    
    df=pd.DataFrame(lines[1:],columns=lines[0])
    #���û����һ����Ҫ�����һ��
    if "����ҽԺ����ҽ��" not in lines[0]:
        df["����ҽԺ����ҽ��"]=""
    #��ӿͻ����һ��
    df["�ͻ����"]="��ҵ˽������ר�ҹҺſ�"
    cxdf = df[df["����״̬"].isin(["ԤԼ����","ԤԼ����"])].copy()
    cxdf = cxdf[["�������","����״̬"]]
    cxdf = cxdf.reset_index()    
    df = df[~df["����״̬"].isin(["ԤԼ����","ԤԼ����"])].copy()
    df = df.reset_index()   
            
    #df1 = pd.read_csv( path+f,',',encoding="gb2312",dtype=coltype) #ֱ�Ӷ��ļ�һֱ���ִ���
    sql = "insert into tgy_xingye_import(�������,����״̬,����,�ֻ���,֤������,������Ŀ,��Ȩ����,���˴���,����Ӧ��,����ҽԺ����ҽ��,��ע,�ͻ����)" 
    sql=sql+" values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    conn = test_orcle()
    cur = conn.cursor()
    
    try:
        cur.execute("alter session set nls_date_format='yyyymmdd'")
        for i,row in df.iterrows():
            cur.execute(sql%(row["�������"],row["����״̬"]
                ,row["����"],row["�ֻ���"],row["֤������"],row["������Ŀ"]
                ,row["��Ȩ����"],row["���˴���"]
                ,row["����Ӧ��"]
                ,row["����ҽԺ����ҽ��"],row["��ע"],row["�ͻ����"]))
        
        #param={'id':2,'n':'admin','p':'password'}
        #cursor.execute('insert into tb_user values(:id,:n,:p)',param);
        ##һ�β����������,����Ϊ�ֵ��б���ʽ
        #param=[{'id':3,'n':'admin','p':'password'},{'id':4,'n':'admin','p':'password'},{'id':5,'n':'admin','p':'password'}];
        #cursor.executemany('insert into tb_user values(:id,:n,:p)',param);    
        #�����ļ������¼
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
    
    #���������Ĵ���
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
            myoutput(file+"��"+str(len(cxdf))+"��ȡ������,���˹�����:")
            myoutput(cxdf)
            #send_mail("test����",file+"��"+str(len(cxdf))+"��ȡ������,���˹�����:"+str(cxdf))
        return False 
    conn.commit()
    
    try:
        #ִ�д洢����,�����ݵ�������
        id = cur.var(cx_Oracle.NUMBER)
        cur.callproc("TGY_XINGYE_ALL_DATA_PROCESS",["SYSTEM",id])
        #����Ƿ���û�е���ϵͳ������
        cur.prepare("select count(*) num from tgy_xingye_import where ���������=1 and DATA_IMPORT_PROCESS_ID=:id")
        cur.execute(None,{'id':id})
        #��ȡһ����¼
        one = cur.fetchone()
        if one[0]>0 :
            myoutput(file+"��"+str(one[0])+"������δ����ϵͳ,����м��")
            myoutput("")        
            #send_mail("test����",file+"��"+str(one[0])+"������δ����ϵͳ,����м��")
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
�ż�����ݵ���ϵͳ
'''    
def deal_team(file):           
    myoutput("����: "+file)
    lines = list(csv.reader(open(file,'r')))
    #�������һ���ظ��ı�ע
    if len(lines)>0 and lines[0][-1]==lines[0][-2]:
       lines[0][-1]="��ע_1"   
    
    df=pd.DataFrame(lines[1:],columns=lines[0])
    #���û����һ����Ҫ�����һ��
    if "����ҽԺ����ҽ��" not in lines[0]:
        df["����ҽԺ����ҽ��"]=""    
    df = df.reset_index()
    
    #df1 = pd.read_csv( path+f,',',encoding="gb2312",dtype=coltype) #ֱ�Ӷ��ļ�һֱ���ִ���
    sql = "insert into tgy_xingye_team_import(�������,����,�ֻ���,֤������,������Ŀ,��Ȩ����,��ע,�ͻ����,��������)" 
    sql=sql+" values('%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    conn = test_orcle()
    cur = conn.cursor()
    
    try:
        cur.execute("alter session set nls_date_format='yyyymmdd'")
        for i,row in df.iterrows():
            cur.execute(sql%(row["�������"]
                ,row["����"],row["�ֻ���"],row["֤������"],row["������Ŀ"]
                ,row["��Ȩ����"],row["��ע"],row["�ͻ����"],row["��������"]))
        
        #param={'id':2,'n':'admin','p':'password'}
        #cursor.execute('insert into tb_user values(:id,:n,:p)',param);
        ##һ�β����������,����Ϊ�ֵ��б���ʽ
        #param=[{'id':3,'n':'admin','p':'password'},{'id':4,'n':'admin','p':'password'},{'id':5,'n':'admin','p':'password'}];
        #cursor.executemany('insert into tb_user values(:id,:n,:p)',param);    
        #�����ļ������¼
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
        #ִ�д洢����,�����ݵ�������
        id = cur.var(cx_Oracle.NUMBER)
        cur.callproc("TGY_XINGYE_TEAM_PROCESS",["SYSTEM",id])
        #����Ƿ���û�е���ϵͳ������
        cur.prepare("select count(*) num from tgy_xingye_team_import where ���������=1 and DATA_IMPORT_PROCESS_ID=:id")
        cur.execute(None,{'id':id})
        #��ȡһ����¼
        one = cur.fetchone()
        if one[0]>0 :
            myoutput(file+"��"+str(one[0])+"������δ����ϵͳ,����м��")
            myoutput("")        
            #send_mail("test����",file+"��"+str(one[0])+"������δ����ϵͳ,����м��")
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
���ȿ��������Ƿ�����������,û�з��������������Ҫ��ʱ�����
'''   
def first_process():
    
    try:
        conn = test_orcle()
        cur = conn.cursor()
        #ִ�д洢����,�����ݵ�������
        id = cur.var(cx_Oracle.NUMBER)
        cur.callproc("TGY_XINGYE_ALL_DATA_PROCESS",["SYSTEM",id])
        #����Ƿ���û�е���ϵͳ������
        cur.prepare("select count(*) num from tgy_xingye_import where ���������=1 and DATA_IMPORT_PROCESS_ID=:id")
        cur.execute(None,{'id':id})
        #��ȡһ����¼
        one = cur.fetchone()
        if one[0]>0 :
            myoutput(file+"��"+str(one[0])+"������δ����ϵͳ,����м��")
            myoutput("")        
            #send_mail("test����",file+"��"+str(one[0])+"������δ����ϵͳ,����м��")
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
        
    #���ȼ�����ݷ�����������ֹ�����ļ�������Ӱ���������
    #������ļ�ʱ����
    rst = first_process()
    if rst is False:
        myoutput("============================")
        myoutput("\033[1;31;47m!!!!!!�봦����ٽ���!!!!!!!!")
        myoutput("============================")
        sys.exit(0)   #�˴�Ϊǿ���˳������д�����������ż������������ע����Ϊ�˱�֤���󲿷ֵ��ӵĴ���
    
    #��ƿ��Һž�ҽ
       
    lck_guahao_jiuyi = cib["lck_guahao_jiuyi"]+today+"/"
    flist = get_todeal_list(lck_guahao_jiuyi)
    for f in flist:
        rst = deal_lck(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!�봦����ٽ���!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)
        else:
            myoutput("OK")    
             
    #��ƿ�������       
    lck_tijian_jieya = cib["lck_tijian_jieya"]+today+"/"
    flist = get_todeal_list(lck_tijian_jieya)
    for f in flist:
        rst = deal_lcktj(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!�봦����ٽ���!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)
        else:
            myoutput("OK")        
    
    #���ÿ��Һ�ԤԼ       
    xyk_guahao = cib["xyk_guahao"]+today+"/"
    flist = get_todeal_list(xyk_guahao)
    for f in flist:
        rst = deal_xyk(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!�봦����ٽ���!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)
        else:
            myoutput("OK")
    
    #���ÿ���ҽ       
    xyk_jiuyi = cib["xyk_jiuyi"]+today+"/"
    flist = get_todeal_list(xyk_jiuyi)
    for f in flist:
        rst = deal_xyk(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!�봦����ٽ���!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)
        else:
            myoutput("OK")                
    
    #���ÿ����       
    xyk_tianjian = cib["xyk_tianjian"]+today+"/"
    flist = get_todeal_list(xyk_tianjian)
    for f in flist:
        rst = deal_xyk(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!�봦����ٽ���!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)
        else:
            myoutput("OK")    
    
    #���ÿ�����       
    xyk_jieya = cib["xyk_jieya"]+today+"/"
    flist = get_todeal_list(xyk_jieya)
    for f in flist:
        rst = deal_xyk(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!�봦����ٽ���!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)  
        else:
            myoutput("OK")   

    #˽�о�ҽ    
    sh_guibin_jiuyi = cib["sh_guibin_jiuyi"]+today+"/"
    flist = get_todeal_list(sh_guibin_jiuyi)
    for f in flist:
        rst = deal_sh(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!�봦����ٽ���!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)  
        else:
            myoutput("OK")        

    #�ż충��
    lck_team = cib["lck_team"]+today+"/"
    flist = get_todeal_list_team(lck_team)
    for f in flist:
        rst = deal_team(f)
        if rst is False:
            myoutput("============================")
            myoutput("\033[1;31;47m!!!!!!�봦����ٽ���!!!!!!!!")
            myoutput("============================")
            #sys.exit(0)  
        else:
            myoutput("OK")  
           
    myoutput("---(::)---")        
    myoutput("---(::)---")
    myoutput("---(::)---")

    #�Զ�����,������Ҫ��Ķ����Զ����в���,����Ҫ�˹���Ԥ
    conn = test_orcle()
    cur = conn.cursor()
    cur.execute("alter session set nls_date_format='yyyymmdd'")
    v_num = cur.var(cx_Oracle.NUMBER)
    cur.callproc("TGY_XINGYE_ZERO_ORDER_DEAL",["SYSTEM",v_num])
    if v_num.getvalue()>0:
        myoutput("�Զ����붩������:"+str(v_num.getvalue()))   
    print("------ע�����������-------")
    sys.exit(0)
    
    