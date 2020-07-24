#%%

# 부모 CLass 경로 설정
import sys
sys.path.append('C:\\Users\\Chan\\.spyder-py3')


#%%

# crawling관련 library 불러오기
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

# 데이터 전처리 및 저장에 필요한 library 불러오기
import pandas as pd
import numpy as np 
import datetime
import baseball.baseball as baseball
from sqlalchemy import create_engine


#%%
class Crawling_baseball(baseball.Database):
    '''
    Class Crawling
    
        KBO에서 제공하는 경기 기록 데이터 크롤링 및 저장하는 Class
        
    '''
    
    
    def __init__(self):
        
        # 팀 번호 dictionary
        self.team_num_dic = {'LG':1,'롯데':2,'KIA':3,'IA':3,'삼성':4,'두산':5,'한화':6,'SK':7,'키움':8,'넥센':8, 'NC':9,'KT':10}
        
        # DB에 넣을 table array생성
        self.game_info_array = np.empty((1,7))
        self.team_game_info_array = np.empty((1,8))
        self.score_array = np.empty((1,18))
        self.batter_array = np.empty((1,14))
        self.pitcher_array = np.empty((1,19))
        
        # 기타 쓰이는 변수
        self.is_start = True # 현재 페이지 시작여부
        self.end = None # 경기종료결과(경기종료, 우천취소 등)
        self.last_game_num_list = list() # 팀 별 마지막 게임 번호 list
        self.team_game_idx_dic = dict() # team_game_idx 생성 후 Home, Away구분해 저장하는 dictionary(데이터 분석용)
        
        
        
    
    def driver_start(self,start_date):
        '''
        Start driver
        
            구글 드라이버 실행
        
        Parameter
        -----------------------------
        
            start_date: 지정한 날짜부터 크롤링 시작
        
        '''
        
        driver = webdriver.Chrome('C:\\Users\\Chan\\Desktop\\chromedriver_win32\\chromedriver')
        driver.get('https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate=' + str(start_date))
        
        self.driver = driver
        self.is_start = True
        
    
    def ready_by_round(self):
        '''
        Set basic setting by round
        
            
            해당 라운드(날짜)에 시작하는 경기들 기본정보 가져오기
            
        '''
        
        # 현재 페이지에서 crawling 할건지 다음페이지로 넘길건지
        if self.is_start: 
            self.is_start = False
            
        
        else: 
            next_button = self.driver.find_element_by_xpath('//*[@id="contents"]/div[2]/ul/li[3]')
            next_button.click()
        
        
        # 페이지소스 가져오기
        page_source = self.driver.page_source
        soup = BeautifulSoup(page_source,'html.parser')
        
        # 경기들에 대한 정보를 가지고있는 elements 가져오기 - 나중에 element로 가져와 매치 별로 사용
        self.elements = self.driver.find_elements_by_class_name('game-cont')
        
        # 날짜 생성        
        date = soup.find('span',{'id':'lblGameDate'})
        date = date.string
        year = date[:4]
        month = date[5:7]
        day = date[8:10]
        self.date_str = year + month + day
        self.year_str = year
        
        # 게임정보 list 생성
        view_port = soup.find('div', {'class' : 'bx-viewport'})
        view_list = view_port.find_all('li')
        self.view_list = view_list
        
        
        
    def ready_by_match(self,match_num,element):
        '''
        Set pagesource by match
        
            매치별 페이지 소스 가져오기
        
        Parameter
        --------------------------------
        
            match_num: 매치번호
            
            element: 매치번호에 맞는 element
            
        '''
        
        # 6번쨰 경기 이상일경우 찾아서 클릭
        if match_num > 4:
            
            self.driver.execute_script("arguments[0].click();", element)
        else:
            element.click()
        WebDriverWait(self.driver,5).until(expected_conditions.presence_of_element_located((By.XPATH,'//*[@id="tabDepth2"]/li[2]')))
        review = self.driver.find_element_by_xpath('//*[@id="tabDepth2"]/li[2]')
        
        
        
        review.click()
        try:
            WebDriverWait(self.driver,5).until(expected_conditions.presence_of_element_located((By.CLASS_NAME,'box-score-area')))
        except:
            review.click()
        page_source = self.driver.page_source
        self.soup = BeautifulSoup(page_source,'html.parser')
        
    
    def create_game_info(self,view):
        '''
        Set game_info array
        
            game_info table 데이터 전처리
            
            columns = ['game_idx', 'home_name', 'away_name', 'stadium', 'end', 'etc']
        
        
        Parameter
        ---------------------
        
            view: 매치별 기본정보 담은 페이지소스
        
        '''
        
        view_str = str(view)
        date_str = self.date_str
        
        home_name = view_str[view_str.index('home_nm'):view_str.index('home_p_id')].strip()[-4:-1].replace('"','')
        away_name = view_str[view_str.index('away_nm'):view_str.index('away_p_id')].strip()[-4:-1].replace('"','')
        
        self.home_name = home_name
        self.away_name = away_name
        
        home_num_str = '%02d' % self.team_num_dic[home_name]
        away_num_str = '%02d' % self.team_num_dic[away_name]
        
        today_game_num = '0' + view_str[view_str.index('game_sc')-3]
        game_idx = date_str + home_num_str + away_num_str + today_game_num
        self.game_idx = game_idx
        
        
        stadium = view.find('span',{'class' : 'place'}).string
        end = view.find('span',{'class':'time'}).string
        
        self.end = end
        etc = None
        game_info_array = np.array([game_idx,home_name,away_name,stadium,end,etc]).reshape(1,-1)
        self.game_info_array = game_info_array
        
    def create_team_game_info(self):
        '''
        Set team_game_info_array
        
            team_game_info table 데이터 전처리
            
            columns = ['game_idx', 'team_game_idx', 'year', 'team_num', 'foe_num', 'game_num', 'home_away']
            
        '''
        year_str = self.year_str
        game_idx = self.game_idx
        
        home_name = self.home_name
        away_name = self.away_name
        
        
        home_num = self.team_num_dic[home_name]
        away_num = self.team_num_dic[away_name]
        
        self.home_away_num_dic = dict()
        self.home_away_num_dic['Home'] = self.team_num_dic[home_name] 
        self.home_away_num_dic['Away'] = self.team_num_dic[away_name]
        
        
        # home_away_game_idx 생성 및 dictionary 저장
        last_home_game_num = self.last_game_num_list[home_num]
        last_away_game_num = self.last_game_num_list[away_num]
        
        home_game_num = last_home_game_num + 1
        away_game_num = last_away_game_num + 1
        
        home_game_idx = year_str + '%02d' % home_num + '%03d' % (home_game_num)
        away_game_idx = year_str + '%02d' % away_num + '%03d' % (away_game_num)
        
        
        self.team_game_idx_dic['Home'] = home_game_idx
        self.team_game_idx_dic['Away'] = away_game_idx
        
        
        home_game_info_array = np.array([game_idx, home_game_idx, int(year_str), home_num, away_num, home_game_num,'home'])
        away_game_info_array = np.array([game_idx, away_game_idx, int(year_str), away_num, home_num, away_game_num,'away'])
        
        self.team_game_info_array = np.vstack([home_game_info_array, away_game_info_array])
        
    def create_score_array(self):
        '''
        Set score_array
        
            score_record table 데이터 가져오기
            
            columns = ['team_game_idx', 'result', 'x1', 'x2', 'x3', 'x4', 'x5', 'x6',
                         'x7', 'x8', 'x9', 'x10', 'x11', 'x12', 'r', 'h', 'e', 'b']
            
        '''
        # 1~12회 이닝별 점수 리스트 생성
        boxscore_find= self.soup.find('div',{'class' : 'tbl-box-score data2'}).find_all('td')
        boxscore_away_list = list()
        boxscore_home_list = list()
        for i,boxscore in enumerate(boxscore_find):
            boxscore = boxscore.string
            
            if i < 12:
                boxscore_away_list.append(boxscore)
            else:
                boxscore_home_list.append(boxscore)
                
        # 경기 득점 / 안타 / 에러 / 볼넷 리스트 생성
        boxscore_run_find= self.soup.find('div',{'class' : 'tbl-box-score data3'}).find_all('td')
        
        boxscore_run_away_list = list()
        boxscore_run_home_list = list()
        for i,boxscore_run in enumerate(boxscore_run_find):
            boxscore_run = boxscore_run.string
            
            if i < 4:
                boxscore_run_away_list.append(boxscore_run)
            else:
                boxscore_run_home_list.append(boxscore_run)
                
        
        # 박스스코어 리스트로 결합
        boxscore_home_list= boxscore_home_list + boxscore_run_home_list
        boxscore_away_list= boxscore_away_list + boxscore_run_away_list
        
        
        
        # 경기 결과 
        self.result_dic = dict()
        if boxscore_home_list[12] > boxscore_away_list[12]:
            home_result = 'win'
            away_result = 'lose'
        elif boxscore_home_list[12] < boxscore_away_list[12]:
            home_result = 'lose'
            away_result = 'win'
        else:
            away_result = 'draw'
            home_result = 'draw'
        
        self.result_dic['Home'] = home_result
        self.result_dic['Away'] = away_result
        
        # 홈팀 / 어웨이팀 정보 및 결과 리스트 생성
        
        
        score_away_list = list()
        score_away_list.append(self.team_game_idx_dic['Away'])
        score_away_list.append(away_result)
        score_away_list.extend(boxscore_away_list)
        
        score_home_list = list()
        score_home_list.append(self.team_game_idx_dic['Home'])
        score_home_list.append(home_result)
        score_home_list.extend(boxscore_home_list)
        
        # score_array에 경기결과list 부착 
        self.score_array = np.vstack([score_away_list,score_home_list])
        
        
        
    def create_batter_array(self,home_away):
        '''
        Set batter_array
        
            batter_record table 데이터 가져오기
            
            columns = ['team_game_idx','bo','po','name','b1','b2','b3','hr','bb',
                       'hbp','ibb','sac','sf','so','go','fo','gidp','etc','h','tbb','ab','pa','xr']
            
        
        Parameter
        --------------------------------
        
            home_away: 홈, 원정 - 데이터가 페이지소스에 Home, Away로 구분됨
            
        '''
        
        # 기록 위치 추적
        batter_basic_find = self.soup.find('table',{'id' : 'tbl' + home_away + 'Hitter1'})
        batter_record_find = self.soup.find('div',{'id':'tbl' + home_away + 'Hitter2'})
        
        batter_num_find  = batter_basic_find.find_all('th')
        batter_name_find = batter_basic_find.find_all('td')
        batter_record_find = batter_record_find.find_all('tr')
        
                
        # 타순 리스트 생성
        num_list = list()
        count = 0
        
        for i,num in enumerate(batter_num_find):
            
            if i < 3:
                continue
            
            num = num.string
            count+=1
            if count % 2 != 0:
                num_list.append(num)
        
        len_batter = len(num_list)

        # 타자 이름 리스트 생성
        name_list = list()
        for i,name in enumerate(batter_name_find):
            if i >= len_batter:
                continue
            
            name = name.string
            name_list.append(name)
            
        # 타자 기록 리스트 생성    
        record_list = list()
        last_round = int(batter_record_find[0].find_all('th')[-1].string)
        
        
        for i,batter in enumerate(batter_record_find):
            if i == 0 or i > len_batter:
                continue
            record_by_batter=  batter.find_all('td')
            record_by_batter_list = list()
            for record in record_by_batter:
                record = str(record)
                where_td = record.find('</td')
                record_by_batter_list.append(record[4:where_td])
                
            record_by_batter_list = record_by_batter_list + ([' '] * (12-last_round))    
            record_list.append(record_by_batter_list)
            
        # 기본정보와 타격기록 취합
        
        name_array = np.array([name_list]).reshape(len_batter,1)
        num_array = np.array([num_list]).reshape(len_batter,1)
        record_array = np.array([record_list]).reshape(len_batter,12)
        result_array = np.hstack([num_array,name_array,record_array])        
        
        self.batter_array = result_array
        
    def create_pitcher_array(self, home_away):
        '''
        Set pitcher_array
        
            pitcher_record table 데이터 가져오기
            
            columns = ['team_game_idx','name', 'po', 'inn', 'tbf', 'np', 'ab', 'h', 'hr', 'tbb', 'so', 'r','er', 'fip']
            
        Parameter
        --------------------------------
        
            home_away: 홈, 원정 - 데이터가 페이지소스에 Home, Away로 구분됨
            
        '''
        
        pitcher_find = self.soup.find('table',{'id' : 'tbl' + home_away + 'Pitcher'}).find_all('tr')

        record_list = list()
        len_pitcher = len(pitcher_find)
        count = 0
        record_by_pitcher_list = list()
        for i,pitcher in enumerate(pitcher_find):
            if i == 0 or i == (len_pitcher-1):
                continue
            pitcher_record_find = pitcher.find_all('td')
            for record in pitcher_record_find:
                record = record.string
                record_by_pitcher_list.append(record)
                count +=1
                if count == 17: # 16가지 투수 기록 다 가져오면 record_list에 투수 기록 append
                    record_list.append(record_by_pitcher_list)
                    record_by_pitcher_list = list()
                    count = 0
        
        len_record = len_pitcher-2
        

        pitcher_array = np.array([record_list]).reshape(len_record,17)
        
        self.pitcher_array = pitcher_array
        
    def get_today(self):
        '''
        Get today_str
        
            오늘 날짜를 8자리 string으로 만들기
            
        '''
        today = datetime.datetime.today()
        year = str(today.year)
        month = str(today.month)
        day = str(today.day)
        if len(month)==1:
            month = '0' + month
            
        if len(day)==1:
            day = '0' + day
        
        today_str = year + month + day
        return today_str
    
    def set_last_game_num_list(self):
        '''
        Set last_game_num_list
        
            DB에서 팀 별 마지막 게임번호 가져오기
            
        '''
        last_game_num_list = list(self.fetch_sql('select total_game_num from team_info'))
        new_list = list()
        for last_game_num in last_game_num_list:
            new_list.append(last_game_num[0])
        self.last_game_num_list = [0] + new_list

#%%
c = Crawling_baseball()
p = baseball.Precleaning()
d = baseball.Database()


#%%
today = c.get_today()
c.driver_start(20200505)
#%%
error_list = list()
error_count = 0
while True:
    
    
    
    c.ready_by_round()
    if c.date_str == c.get_today():
        break
    else:
        
        try:
        
            for j, element in enumerate(c.elements):
                
                # last_game_num list 생성
                c.set_last_game_num_list()
                view = c.view_list[j]
                
                # game_info 크롤링 및 저장
                c.create_game_info(view)
                d.array_to_db(c.game_info_array, 'game_info')
                
                if c.end == '경기종료':
                    c.ready_by_match(j,element) # 매치별 기본정보 구하기
                    
                    # team_game_info & score_record 크롤링 및 저장
                    c.create_team_game_info()
                    c.create_score_array()
                    
                    d.array_to_db(c.team_game_info_array,'team_game_info')
                    d.array_to_db(c.score_array,'score_record')
                    for home_away in ['Home', 'Away']:
                        
                        team_game_idx = c.team_game_idx_dic[home_away]
                        
                        # batter_record 크롤링 및 저장
                        c.create_batter_array(home_away)
                        p.batter_raw_array = c.batter_array
                        p.set_batter_array(team_game_idx)
                        
                        d.array_to_db(p.batter_array,'batter_record')
                        
                        # pitcher_record 크롤링 및 저장
                        c.create_pitcher_array(home_away)
                        p.pitcher_raw_array = c.pitcher_array
                        p.set_pitcher_array(team_game_idx)
                        d.array_to_db(p.pitcher_array,'pitcher_record')
                        
                        # 게임번호 +1
                        team_num = c.home_away_num_dic[home_away]
                        c.last_game_num_list[team_num]+=1
                
                c.update_total_game_num(c.last_game_num_list)
                error_count = 0 
        
        # 페이지 창 뜨는 속도가 느려서 오류먹을떄가있음 다시 실행하자
        except:
            c.is_start = True
            error_count+=1
            
        # 오류 3번 이상이면 문제 있다고 판단 
        if error_count >3:
            print('error!!!!!!!!!!')
            break
        
