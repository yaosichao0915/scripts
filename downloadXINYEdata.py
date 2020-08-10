#python32 -m "d:\cib\downloadXINYEdata.py N"
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import filecmp
import logging
import shutil
import colorama
import sys
from datetime import datetime, timedelta
from colorama import init,Fore,Back,Style
init(autoreset=True)

temp_dir = 'D:/cib/temp/'
screen_shot_dir = 'D:/cib/screenshot/'

cib_code={
"xyk_tianjian":"2020105919",
"xyk_jieya":"2020105917",
"xyk_jiuyi":"2020105918",
"xyk_guahao":"2020105916",
"lck_tijian_jieya":"2019100521",
"lck_guahao_jiuyi":"2019100524"
}
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
now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
today_date = time.strftime("%Y%m%d", time.localtime())
logging.basicConfig(level=logging.INFO,
                    filename='C:/cib/log/'+today_date+"download.log",
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(asctime)s - %(message)s')

def myoutput(msg):
    logger = logging.getLogger("downloaddata") 
    logger.info(msg)
    print(msg)

def clean_screen_shot():
    try:
        now_time = time.time()
        duration = 7
        for file in os.listdir(screen_shot_dir):
            m_time = os.stat(screen_shot_dir+file).st_mtime
            if now_time - m_time > 3600*24*duration:
                os.remove(screen_shot_dir+file)
    except Exception as e:
        myoutput(e)

def capture_screen_shot(driver,item):
    try:
        
        file_name = time.strftime("%Y%m%d-%H-%M-%S", time.localtime(time.time()))
        file_dst = screen_shot_dir+item+file_name+'.png'
 
        driver.save_screenshot(file_dst)
    except Exception as e:
        myoutput(e)
        myoutput('注意！截图错误！')
        
def go_to_page(driver):
    page = ['2019100000','2019100001','2019100016','2019100042','2019100150'] #CIB 总行 普惠金融事业部 权益  qyxt
    for page_id in page:
        try:
            element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//tr[@data-row-key=%s]//div[@class='white']"%page_id))
            )           
        finally:
            driver.find_element_by_xpath("//tr[@data-row-key=%s]//div[@class='white']"%page_id).click()
            time.sleep(1)

def file_download(driver,item,date):
    try:
        try:
            element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//tr[@data-row-key=%s]//div[@class='white']"%cib_code[item]))
            )           
        finally:
            driver.find_element_by_xpath("//tr[@data-row-key=%s]//div[@class='white']"%cib_code[item]).click() #体检
            time.sleep(1)
        try:
            
            driver.find_element_by_xpath("//div[@class='ant-table-selection']//input[@type='checkbox']").click()
            capture_screen_shot(driver,item) #截图
            time.sleep(1)
            driver.find_element_by_xpath("//div[@style='cursor: pointer;']//div[contains(text(),'%s')]"%date).click()
        except:
            driver.find_element_by_xpath("//div[@class='return']").click()
            time.sleep(2)
            return 2
        time.sleep(1)
        
        driver.find_element_by_xpath("//div[@class='ant-table-selection']//input[@type='checkbox']").click()
        capture_screen_shot(driver,item) #截图
        #driver.find_element_by_xpath("//tr[@class='ant-table-row ant-table-row-level-0'][1]//input[@type='checkbox']").click()
        driver.find_element_by_xpath("//li[@class='download']").click()
        time.sleep(1.5)
        driver.find_element_by_xpath("//button[@class='el-button el-button--default el-button--small el-button--primary ']").click()
        time.sleep(2.5)
        driver.get('https://180.153.144.209/cftm/')
        time.sleep(3)
        try:
            driver.switch_to.alert.accept()
        except:
            print('刷新失败')
        return 0
    except:
        driver.find_element_by_xpath("//div[@class='return']").click()
        time.sleep(2)
        driver.find_element_by_xpath("//div[@class='return']").click()
        time.sleep(2)
        return 1

def deal_downloadfile(item):
#下载等待
    try:
     #   time.sleep(1)
        seconds = 0
        dl_wait = True
        while dl_wait and seconds < 10: 
            time.sleep(1)
            dl_wait = False
            f_list = os.listdir(temp_dir)
            for fname in f_list:
                if fname.endswith('.crdownload'):
                    dl_wait = True
            seconds += 1
            if seconds > 9:
                myoutput('\033[1;31;47m%s下载超时错误'%cib[item])
        #对比
        for f in f_list:
            dow=temp_dir+f
            dst=cib[item]+today_date+'/'+f
            dst_dif = cib[item]+today_date+'/'+'new_'+f
            try:
                if filecmp.cmp(dow,dst):
                    os.remove(dow)
                else:
                    shutil.move(dow,dst_dif)
                   # myoutput('%s内容不一致，请人工审查'%dst)
                    myoutput('\033[1;31;47m%s内容不一致，请人工审查'%dst)
            except:            
                shutil.move(dow,dst)
                myoutput('成功新增 %s'%dst)    
        return 0
    except Exception as e:
        myoutput(e)
        return 1
            
try:
    try:
        shutil.rmtree(temp_dir)
        os.mkdir(temp_dir)
    except:
        os.mkdir(temp_dir)
    try:
        os.mkdir(screen_shot_dir)
    except:
        clean_screen_shot()   
    if os.path.exists(cib['xyk_tianjian']+today_date+'/'):
        pass
    else:
        myoutput('\033[1;31;47m 请先创建日期文件夹')       
        sys.exit(0)
    if sys.argv[1] == 'M':    
        gap = input("请输入假期有几天，普通双休日即输入2:\n")
        gap = int(gap)+1
    else:
        gap=1
    crawl_date=[]
    for i in range(gap):
        crawl_date.append(datetime.strftime(datetime.now() - timedelta(i),"%Y%m%d"))
    myoutput('即将下载%s'%crawl_date)
    chrome_options = webdriver.ChromeOptions() 
    chrome_options.add_argument("user-data-dir=C:\\Users\\TGY_DataTransfer\\AppData\\Local\\Google\\Chrome\\User Data\\Default") 
    chrome_options.add_argument('--start-maximized')
    prefs = {'download.default_directory' : 'D:\\cib\\temp',
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True}
    chrome_options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome('D:\\cib\\chromedriver.exe',options=chrome_options)
    driver.get('https://180.153.144.209/cftm/')
    time.sleep(10)

    #等待验证码输入，检测是否已已经跳转
    while(1):
        try:
            driver.find_element_by_xpath("//input[@type='password']")
            time.sleep(1)
        except:
            time.sleep(2)
            break

    go_to_page(driver)  #进入主页面

    #根据类别下载和对比
    for item in cib_code:
        
        #下载 
        for date in crawl_date:
            result = file_download(driver,item,date)  #根据每个类别和日期下载
            if result==1:
                myoutput('\033[1;31;47m%s----下载错误，请手动下载或再次运行'%cib[item])
            if result==2:
                myoutput('%s----该日期没有数据'%cib[item])
            if result==0:
                go_to_page(driver)
                if deal_downloadfile(item)==0:
                    myoutput('%s----下载当日数据成功'%cib[item])
                else:
                    myoutput('\033[1;31;47m%s----下载文件处理错误，请手动下载或再次运行'%cib[item])
                        
    myoutput('---------下载数据任务结束---------')
    #driver.quit()
    print("------检查红字提醒，检查结束后注意关闭该浏览器，私行请用另一浏览器，按任意键退出------")
    #sys.exit(0)
except Exception as e:
    shutil.rmtree(temp_dir)
    os.mkdir(temp_dir)
    myoutput('\033[1;31;47m存在未知错误，请联系管理员')
    myoutput(e)