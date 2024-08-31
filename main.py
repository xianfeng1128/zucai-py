import requests
from bs4 import BeautifulSoup
import pandas as pd
import schedule
import time
from datetime import datetime
import os
import logging

class SafeFileHandler(logging.FileHandler):
    def emit(self, record):
        if not os.path.exists(self.baseFilename):
            self.stream = self._open()
        logging.FileHandler.emit(self, record)
        self.reverse_log_file()

    def reverse_log_file(self):
        if os.path.exists(self.baseFilename):
            with open(self.baseFilename, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            lines.reverse()
            with open(self.baseFilename, 'w', encoding='utf-8') as f:
                f.writelines(lines)

log_file = "D:\\react\\zucai\\scraping_log.txt"
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        SafeFileHandler(log_file, encoding='utf-8'),
                        logging.StreamHandler()
                    ])

def scrape_data():
    url = 'https://www.okooo.com/zucai/'
    try:
        response = requests.get(url)
        response.encoding = 'gb2312'

        if response.status_code == 405:
            logging.error(f"被网站拦截。状态码: {response.status_code}")
            return None, None, True
        elif response.status_code != 200:
            logging.error(f"无法获取网页内容。状态码: {response.status_code}")
            return None, None, False

        soup = BeautifulSoup(response.text, 'html.parser')

        period_element = soup.find(class_='top')
        if not period_element:
            logging.error("未找到期数数据。网页结构可能已更改。")
            return None, None, False
        period = period_element.text.strip()

        home_teams = [element.text.strip() for element in soup.find_all(class_='homenameobj homename')]
        away_teams = [element.text.strip() for element in soup.find_all(class_='awaynameobj awayname')]
        noborder0 = [element.text.strip() for element in soup.find_all(class_='noborder0')]
        noborder1 = [element.text.strip() for element in soup.find_all(class_='noborder1')]
        noborder2 = [element.text.strip() for element in soup.find_all(class_='noborder2')]

        if not home_teams or not away_teams:
            logging.error("未找到比赛数据。网页结构可能已更改。")
            return None, None, False

        data = {
            '主队': home_teams,
            '客队': away_teams,
            'Noborder0': noborder0,
            'Noborder1': noborder1,
            'Noborder2': noborder2
        }
        df = pd.DataFrame(data)
        return df, period, False
    except Exception as e:
        logging.error(f"抓取数据时发生错误: {e}")
        return None, None, False

def save_data(df, timestamp, period):
    file_path = 'D:\\react\\zucai\\data.xlsx'

    formatted_timestamp = datetime.now().strftime('%m-%d %H:%M:%S')

    try:
        all_sheets_data = {}

        if os.path.exists(file_path):
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                all_sheets_data[sheet_name] = pd.read_excel(file_path, sheet_name=sheet_name)

        reshaped_data = []
        for i in range(len(df)):
            reshaped_data.append([df['主队'][i], df['客队'][i], 'Noborder0', df['Noborder0'][i]])
            reshaped_data.append([df['主队'][i], df['客队'][i], 'Noborder1', df['Noborder1'][i]])
            reshaped_data.append([df['主队'][i], df['客队'][i], 'Noborder2', df['Noborder2'][i]])

        reshaped_df = pd.DataFrame(reshaped_data, columns=['主队', '客队', '比分类型', formatted_timestamp])

        if period in all_sheets_data:
            existing_df = all_sheets_data[period]
            existing_df[formatted_timestamp] = reshaped_df[formatted_timestamp]
            all_sheets_data[period] = existing_df
        else:
            all_sheets_data[period] = reshaped_df

        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            for sheet_name, data in all_sheets_data.items():
                data.to_excel(writer, sheet_name=sheet_name, index=False)
            logging.info(f"数据已成功保存到文件。")
    except Exception as e:
        logging.error(f"保存数据时发生错误: {e}")

def job():
    while True:
        try:
            df, period, should_retry = scrape_data()
            if df is not None and period is not None:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                save_data(df, timestamp, period)
                logging.info(f"数据已成功抓取并保存")
                break
            elif should_retry:
                logging.warning(f"抓取失败 即将重试。")
                time.sleep(15)
            else:
                logging.warning(f"抓取失败 将在下次调度时重试。")
                break
        except Exception as e:
            logging.error(f"发生错误: {e}。15秒后重试。")
            time.sleep(15)

for minute in range(5, 60, 10):
    schedule.every().hour.at(f":{minute:02d}").do(job)

logging.info("定时任务已启动，每到5分刻时抓取一次数据")
while True:
    schedule.run_pending()
    time.sleep(1)
