#%%
import numpy as np
import pandas as pd
import pymysql
from sqlalchemy import create_engine
#%%
class Baseball:
    '''
    공통적으로 쓰이는 값들을 선언한 부모 Class
    
        team_dic : 팀이름을 번호에 매칭하는 dictionary
    
        year_list : 분석에 사용하는 2017-2019년도 저장 list
    
        address : 데이터 주소 참조
    '''
    team_dic = {'LG':1,'롯데':2,'KIA':3,'삼성':4,'두산':5,'한화':6,'SK':7,'키움':8,'넥센':8,'NC':9,'KT':10,'kt':10}
    
    year_list = [2017,2018,2019]
    park_factor_2016 = {'잠실': 0.954,'사직': 1.030,'광주':0.997,'대구':1.042, '대전':1.007,'문학':1.016,'고척':1.032,'마산':0.943,'수원':1.021}
    address = 'C:\\Users\\Chan\\Desktop\\BaseballProject\\data'
    
    
    def __init__(self):
        return

     
    def save_csv(self,data,address,name):
        '''
        csv로 데이터 저장하기
        '''
        data = pd.DataFrame(data)
        data.to_csv(address + '\\' + name + '.csv',encoding = 'cp949')
        
    
    def load_csv(self,address,name):
        '''
        csv 데이터 불러오기
        '''
        data = pd.read_csv(address + '\\' + name + '.csv',encoding = 'cp949' )
        result = np.array(data)
        return result
    


class Precleaning(Baseball):
    '''

    데이터 전처리 class
    크롤링을 통해 뽑은 raw데이터를 분석에 용이한 형태로 전처리
    
    
    Raw Array before Set Array
    --------------------------
        batter_raw_array(n x 16) : [bo','name','X1','X2','X3','X4','X5','X6','X7','X8','X9','X10','X11','X12']
        pitcher_raw_array(n x 19) : ['name','position','result','win','lose','save',ip','tbf','np','ab','h','hr','tbb','so','r','er','era']
        
        
    

    
    Set Array
    ---------
        batter_array(n x 18) : ['date','team','bo','name','h1','h2','h3','hr','bb','hbp','ibb','sac','sf','so','go','fo','gidp','etc','h','tbb','ab','pa']
        pitcher_array(n x 14) : ['date','team','name','position','ip','tbf','np','ab','h','hr','tbb','so','r','er']
        
    '''
    def __init__(self):
               
        self.batter_raw_array = None
        self.pitcher_raw_array = None
        self.score_raw_array = None
        
    
    
    
    def set_batter_array(self,team_game_idx):
        '''
        Set batter_array(n x 18)
        
            타자 raw data 전처리
            이닝별로 기록된 타자데이터 -> 기록별 데이터 (n x 18)
        
        Values
        -------
        
            batter_basic_array(n x 4): [date, team, bo, name]
            batter_record_array(n x 14): [h1, h2, h3, hr, bb, hbp, ibb, sac, sf, so, go, fo, gidp, etc, h, tbb, ab, pa]
        
        '''
        batter_array = self.batter_raw_array
        info_array = batter_array[:,:2]
        batter_record_array = batter_array[:,2:]
        
        # 기록을 저장하는 record_dic생성 및 데이터 할당(ex: {우중안:0, 우중2:1})
        # batter_records: 경기별 타자의 1~12회까지 기록 (1 x 12)
        record_dic = dict()
        old_record_array = np.zeros((1,14))
        for batter_records in batter_record_array:
            new_record_array = np.zeros(14)
            for records in batter_records:
                records = str(records)
                records = records.split('<br/>/ ')
                for record in records:
                    
                    record_num = record_dic.get(record)
                    
                    
                    if record_num != None:
                        new_record_array[record_num]+=1
                        
                    # record_dic에 값이 존재하지 않으면 add to key
                    else:
                        
                        if record[-1] == '안': record_dic[record] = 0 # 1루타: 0
                        elif record[-1] == '2': record_dic[record] = 1 # 2루타: 1
                        elif record[-1] == '3': record_dic[record] = 2 # 3루타: 2
                        elif record[-1] == '홈': record_dic[record] = 3 # 홈런: 3    
                        elif record == '4구': record_dic[record] = 4 # 볼넷: 4
                        elif record == '사구': record_dic[record] = 5 # 사구: 5
                        elif record == '고4': record_dic[record] = 6 # 고의사구: 6
                        elif record[-2:] in ['희번','희실','희선']: record_dic[record] = 7 # 희생번트: 7
                        elif record[-2:] == '희비' : record_dic[record] = 8 # 희생타: 8
                        elif record == '삼진' or record == '스낫': record_dic[record] = 9 # 삼진: 9
                        elif record[-1] in ['땅','번'] or record in ['포실','투실','1실','2실','3실','유실','야선']: record_dic[record] = 10 # 그라운드아웃: 10
                        elif record[-1] in ['파','비','직','실']: record_dic[record] = 11 # 플라이아웃: 11
                        elif record[-1] == '병' or record[-2:] == '삼중': record_dic[record] = 12 # 병살타: 12
                        elif record not in ['nan', '', ' ']: record_dic[record] = 13 # ETC: 13   
                        
                        if record not in ['nan','',' ']:
                            new_record_array[record_dic[record]] +=1
            
            old_record_array = np.vstack([old_record_array,new_record_array])
            
        
        record_array = old_record_array[1:,:] # record_array = (n,14)
        
        # h,tbb,ab,pa 계산을 위한 new_matrix생성(14x4) 및 new_record_array생성 (n,14) x (14,4) = (n,4)
        
        # h = h1 + h2 + h3 + h4
        h_array = np.hstack([np.ones(4),np.zeros(10)])
        # tbb = bb + hbp + ibb
        tbb_array = np.hstack([np.zeros(4),np.ones(3),np.zeros(7)])
        # ab = h + so + go + fo + gidp
        ab_array = np.hstack([np.ones(4),np.zeros(5),np.ones(4),np.zeros(1)])
        # pa = ab + tbb + sf
        pa_array = ab_array + tbb_array
        pa_array[8] = 1
        
        new_matrix_array = np.transpose(np.vstack([h_array,tbb_array,ab_array,pa_array]))
        
        new_record_array = record_array.dot(new_matrix_array) # new_record_array = (n,4)
        
        total_record_array = np.hstack([record_array,new_record_array]) # total_record_array = (n,18)
        
        # GO값 보정: go+= sac + gidp
        total_record_array[:,10] += total_record_array[:,7] + total_record_array[:,12]
        
        # FO값 보정: fo+= sf
        total_record_array[:,11] += total_record_array[:,8]
        
        # XR생성
        
        xr_factor_array = np.array([0.5, 0.72, 1.04, 1.44, 0.34, 0.34, -0.09, 0.04, 0.37, -0.008, 0, 0, -0.37, 0, 0.09, 0, -0.09, 0])
        xr_array = np.round(np.dot(total_record_array,xr_factor_array).reshape(-1,1),3)
        
        po_list = list()
        last_bo = 0
        po = 1
        for batter in batter_array:
            bo = int(batter[0])
            
            if last_bo == bo:
                po +=1
                
            else:
                po = 1
            
            last_bo = bo
            po_list.append(po)
        
        po_array = np.array(po_list).reshape(-1,1)
        info_array = np.hstack([po_array,info_array])
        info_array[:,(0,1)] = info_array[:,(1,0)] 
        
        
        len_batter_array = len(po_list)
        team_game_idx_array = np.full((len_batter_array,1),team_game_idx)
        
        batter_array = np.hstack([team_game_idx_array, info_array,total_record_array,xr_array])            
        self.batter_array = batter_array       
        
        
    
    
    def set_pitcher_array(self,team_game_idx):
        '''
        Set pitcher_array(n x 14) 
        
            투수 raw데이터 전처리    
            


        Values
        ------
        
            info_array(n x 4) : name, position
            inn_array(n x 1): inn
            record_array(n x 9) : tbf, np, ab, h, hr, tbb, so, r, er
            fip_array(n x 1) : fip
            
            return pitcher_array(n x 14)
            
            
        '''
        pitcher_array = self.pitcher_raw_array
        info_array = pitcher_array[:,:2]
        inn_array = pitcher_array[:,6]
        record_array = pitcher_array[:,7:-1].astype(np.float)
        
        
        # inn_array 소수점으로 변경
        new_list = list()
        for inn in inn_array:
            if len(inn)==1:
                inn = int(inn)
            elif len(inn)==3:
                inn = int(inn[0]) * 0.333
            elif len(inn):
                inn = int(inn[0]) + int(inn[2])*0.333
            new_list.append(inn)
        inn_array = np.array(new_list).reshape(-1,1)
        
        # SP, RP 구분
        position_array = info_array[:,1]
        po_list = list()
        for po,position in enumerate(position_array):
            po_list.append(po+1)
            
        
        info_array[:,1] = np.array(po_list)
        '''
        # csv파일의 IP column의 1/3, 2/3을 날짜로 인식해 숫자로 변경
        inn_array = pitcher_record_array[:,0]
        inn_array = np.where(inn_array == '43833',str(1/3),inn_array)
        inn_array = np.where(inn_array == '43864',str(2/3),inn_array)
        pitcher_record_array[:,0] = inn_array
        '''
        
        
        # fip = (((13 * HR) + (3 * TBB) - (2 * SO)) / IP) + 3.2
        # 추후 IP로 나누고 3.2더하는 작업필요
        fip = (record_array[:,4]*13 + record_array[:,5]*3 - record_array[:,6]*2)
        
        fip_array = fip.reshape(-1,1)
        
        len_pitcher_array = len(inn_array)
        team_game_idx_array = np.full((len_pitcher_array,1),team_game_idx)
        
        pitcher_array = np.hstack([team_game_idx_array, info_array, inn_array, record_array,fip_array])
        
        self.pitcher_array = pitcher_array
    
   
    
    def save_array(self):
        '''
        Set & Save batter_array / pitcher_array / score_array 
         
            raw_array를 array로 set 하고 저장하는 함수
            저장완료 사용 x
        '''
        address = self.address
        # raw_array를 array로 set
        self.batter_raw_array = self.load_csv(address,'batter_raw_data')
        self.set_batter_array()
        self.pitcher_raw_array = self.load_csv(address,'pitcher_raw_data')
        self.set_pitcher_array()
        self.score_raw_array = self.load_csv(address,'score_raw_data')
        self.set_score_array()
        
        # set한 array를 csv파일로 저장
        self.save_csv(self.batter_array, address,'batter_data')
        self.save_csv(self.pitcher_array, address,'pitcher_data')
        self.save_csv(self.score_array, address,'score_data')
        
    def set_toto_array(self):
        
        toto_array = self.load_csv(self.address,'crawling_toto_baseball')
        normal_toto_array = np.zeros((1,19))
        handi_toto_array = np.zeros((1,19))
        unover_toto_array = np.zeros((1,19))
        
        
        
        for new_array in toto_array:
            new_array = np.hstack([new_array,self.team_dic[new_array[5]],self.team_dic[new_array[6]]])
            if new_array[3] == 1:
                normal_toto_array = np.vstack([normal_toto_array,new_array])
            elif new_array[3] == 2:
                handi_toto_array = np.vstack([handi_toto_array, new_array])
            elif new_array[3] == 3:
                unover_toto_array = np.vstack([unover_toto_array, new_array])
                
        normal_toto_array = normal_toto_array[1:,:]
        handi_toto_array = handi_toto_array[1:,:]
        unover_toto_array = unover_toto_array[1:,:]
            
        self.save_csv(normal_toto_array,self.address,'normal_toto_array')
        self.save_csv(handi_toto_array,self.address,'handi_toto_array')
        self.save_csv(unover_toto_array,self.address,'unover_toto_array')
    
    
    ################################    DB관련코드    #############################################
    
    
class Database():
    def __init__(self):
        self.conn = None
        self.team_dic = {'LG':1,'롯데':2,'KIA':3,'삼성':4,'두산':5,'한화':6,'SK':7,'키움':8,'NC':9,'KT':10}
        self.stadium_dic = {'LG':'잠실','롯데':'사직','KIA':'광주','삼성':'대구','두산':'잠실','한화':'대전','SK':'문학','키움':'고척','넥센':'고척','NC':'마산','KT':'수원','kt':'수원'}
    def set_conn(self):
        self.conn = pymysql.connect(host='localhost', user='root', password='dudrn1', db='baseball', charset='utf8')
    def to_sql(self,sql):
        self.set_conn()
        conn = self.conn
        cursor = conn.cursor()
        
        sql = sql
        cursor.execute(sql)
        conn.commit()
        conn.close()
        
    def fetch_sql(self,sql):
        self.set_conn()
        conn = self.conn
        cursor = conn.cursor()
        
        sql = sql
        cursor.execute(sql)
        result = cursor.fetchall()
        conn.commit()
        conn.close()
        return result
    
    def update_total_game_num(self,update_game_num_list):
        self.set_conn()
        conn = self.conn
        cursor = conn.cursor()
        for team_num in range(1,11):
            update_game_num = update_game_num_list[team_num]
            sql = "update team_info set total_game_num =" + str(update_game_num) + " where team_num ="+ str(team_num)
            cursor.execute(sql)
        
        conn.commit()
        conn.close()
        
    
    
    
    def array_to_db(self,data_array,name):
        
        if name == 'game_info':
            
            columns = ['game_idx', 'home_name', 'away_name', 'stadium', 'end', 'etc']
            
        elif name == 'team_game_info':
            
            columns = ['game_idx', 'team_game_idx', 'year', 'team_num', 'foe_num', 'game_num', 'home_away']
            
        elif name == 'score_record':
            
            columns = ['team_game_idx', 'result', 'x1', 'x2', 'x3', 'x4', 'x5', 'x6',
                         'x7', 'x8', 'x9', 'x10', 'x11', 'x12', 'r', 'h', 'e', 'b']
            
        elif name == 'batter_record':
            
            columns = ['team_game_idx','bo','po','name','b1','b2','b3','hr','bb',
                   'hbp','ibb','sac','sf','so','go','fo','gidp','etc','h','tbb','ab','pa','xr']
        
        elif name =='pitcher_record':
            
            columns = ['team_game_idx','name', 'po', 'inn', 'tbf', 'np', 'ab', 'h', 'hr', 'tbb', 'so', 'r','er', 'fip']
            
        data_pd = pd.DataFrame(data_array,columns = columns)
        
        engine = create_engine("mysql+pymysql://root:" + "dudrn1" + "@127.0.0.1/baseball",encoding = 'utf-8')
        
        data_pd.to_sql(name=name,con = engine,if_exists ='append',index = False)
        conn = engine.connect()
        conn.close()