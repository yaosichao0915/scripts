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
from colorama import init,Fore,Back,Style
init(autoreset=True)

temp_dir = 'D:/cib/temp/'
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

def file_download(driver,section,date):
    try:
        try:
            element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//tr[@data-row-key=%s]//div[@class='white']"%section))
            )           
        finally:
            driver.find_element_by_xpath("//tr[@data-row-key=%s]//div[@class='white']"%section).click() #体检
            time.sleep(1)
        try:
            driver.find_element_by_xpath("//div[@style='cursor: pointer;']//div[contains(text(),'%s')]"%today_date).click()
        except:
            driver.find_element_by_xpath("//div[@class='return']").click()
            time.sleep(1)
            return 2
        time.sleep(1)
        driver.find_element_by_xpath("//div[@class='ant-table-selection']//input[@type='checkbox']").click()
        #driver.find_element_by_xpath("//tr[@class='ant-table-row ant-table-row-level-0'][1]//input[@type='checkbox']").click()
        driver.find_element_by_xpath("//li[@class='download']").click()
        time.sleep(1)
        driver.find_element_by_xpath("//button[@class='el-button el-button--default el-button--small el-button--primary ']").click()
        time.sleep(2)
        driver.find_element_by_xpath("//div[@class='return']").click()
        time.sleep(1)
        driver.find_element_by_xpath("//div[@class='return']").click()
        time.sleep(1)
    except:
        driver.find_element_by_xpath("//div[@class='return']").click()
        time.sleep(1)
        driver.find_element_by_xpath("//div[@class='return']").click()
        time.sleep(1)
        return 1
    return 0

try:    
    chrome_options = webdriver.ChromeOptions() 
    chrome_options.add_argument("user-data-dir=C:\\Users\\TGY_DataTransfer\\AppData\\Local\\Google\\Chrome\\User Data\\Default") #Path to your chrome profile
    prefs = {'download.default_directory' : 'D:\\cib\\temp',
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True}
    chrome_options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome('D:\\cib\\chromedriver.exe',options=chrome_options)
    driver.get('https://180.153.144.209/cftm/')
    time.sleep(10)

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
        result = file_download(driver,cib_code[item],today_date) 
        if result==1:
            myoutput('\033[1;31;47m%s----下载错误'%cib[item])
        if result==2:
            myoutput('%s----该日期没有数据'%cib[item])
        if result==0:
            myoutput('%s----下载当日数据成功'%cib[item])
        #对比 
        time.sleep(3)
        list = os.listdir(temp_dir)
        for f in list:
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
    myoutput('---------下载数据任务结束---------')
    driver.quit()
    input("------检查红字提醒，按任意键退出------")
except Exception as e:
    myoutput(e)