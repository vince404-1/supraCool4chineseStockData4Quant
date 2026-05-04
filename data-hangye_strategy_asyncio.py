"""
data-hangye_strategy_asyncio.py

Author: VZ
Version: 1.1.0 
Date: 2025-11-18

Changelog:
- v1.1.0  (2025-11-24): add value and funcion for check and update namespace data
- v1.1.1  (2025-12-07): for improve namespace data update
- v1.1.2  (2025-12-14): fix namespace downloading pro.dividend(ts_code=  code+"."+market )  🤜 pro.namechange(....)
- v1.1.3  (2026-01-22): zhuyin_dict_ add df.end_date.astype(int) avoid reapt data  ,
			if len(tp1_)==0 add in loadng_pepb_data() istead of if len(  datapepb2[ code_]  )==0:continue
			loadng_pepb_data() if upadate next_day_ == today add to donlist
Dependencies: ***
"""

import json
import pandas as pd
import tushare as ts
import akshare as ak
from tqdm import tqdm 
import copy
import pickle
from datetime import datetime,timedelta
import os
from openai import OpenAI
import json
import numpy as np
import time
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning) 
ts.set_token('*****')   ##upload by yours
pro = ts.pro_api()
import asyncio
import time
from dataclasses import dataclass
from typing import Callable, Coroutine
from json_repair import repair_json
import re
import shutil
import zipfile


##############参数########
stock_list = []
stock_list
code_list =[]
#code_list= code_list[:15]
today_ =""
donlist_p = []
donlist_fh = []
donlist_pepb = []
donlist_zhuyin = []
donlist_ns =[] 
tp_counter=0
data_priec={}   #stock daily price dict
datapepb2={}    #pepb daily dict
fenhong_dict_  = {} #fenghong dict
isUpadateFh=True  #is update the fenghong data in this time
isUpadateZhuyin=True  #is update the Zhuyin data in this time
zhuyin_dict_ = [] #zhu yin dict  
zhuyin_hangye_dict={}
namespace_dict ={} #store name space  #v1.1.1 - 2025-09-24
data_ts=''  #for store tuishidata df #v1.1.1 - 2025-09-24
zhuyin_dict_check_count=0
combined_df=[]
backup_ds_path ="/home/vincezlab/quantData_dload1/backup/"
#backup_ds_path ="F:/Qunat/loadingds_draft/backup/"
load_ds_path="/home/vincezlab/quantData_dload1/newest/"
#load_ds_path="F:/Qunat/loadingds_draft/newest/"
save_ds_path="/home/vincezlab/quantData_dload1/tempo/"
#save_ds_path="F:/Qunat/loadingds_draft/tempo/"


pc_count=0
pb_count=0
fh_count=0
zz_count=0
ns_count=0  #v1.1.1 - 2025-09-24
############################


def load_dict(save_name_):
    with open(save_name_, 'r') as f:
        loaded = pickle.load(f)
    # 将每个 JSON 字符串转回 DataFrame
    for key in loaded:
        loaded[key] = pd.read_json(loaded[key])
    return loaded

def save_dict(usedict_, save_name_):
    for t in usedict_.keys():
        usedict_[t]  = usedict_[t].to_json() 
    with open(save_name_, 'w') as f:
        json.dump(usedict_, f )
    print( len(usedict_.keys() )  )

def identify_exchange(stock_code):
    code = str(stock_code).zfill(6)  # 转换为6位字符串，补前导零
    # 上证判断条件
    if code.startswith(('600', '601', '603', '688','605')):
        return 'SH'
    # 深证判断条件
    elif code.startswith(('000', '001', '002','003', '300','301')):
        return 'SZ'
    elif code.startswith(('83','87','43','9')):
        return 'BJ'
    else:
        return 'Unknown'  # 其他情况（如B股、北交所等）

def pre_process_ak2ts(usedf_):
    usedf_['ts_code']= usedf_['ts_code'].str.split(".").str[0]
    usedf_['amount']= usedf_['amount']*1000
    usedf_['振幅'] = ( usedf_['high']-  usedf_['low']) / usedf_['pre_close']*100 
    usedf_ = usedf_.rename(columns={"trade_date": "日期", "ts_code": "股票代码", "open": "开盘", "high": "最高"
        , "low": "最低", "close": "收盘" , "change": "涨跌额"  , "pct_chg": "涨跌幅", "vol": "成交量", "amount":"成交额" , "turnover_rate":"换手率"  })
    return usedf_[ ['日期', '股票代码', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额',       '换手率'] ] 

def is_valid_json(json_str):
    try:
        json.loads(json_str)
        return True
    except json.JSONDecodeError:
        return False

def robust_json_extract(text):
    # 1. 提取可能包含 JSON 的文本块
    matches = re.findall(r'\{[\s\S]*?\}|\[[\s\S]*?\]', text)
    for match in matches:
        try:
            # 2. 修复 JSON 字符串
            repaired = repair_json(match)
            # 3. 解析为 Python 对象
            return json.loads(repaired)
        except Exception:
            continue
    return None


def zip_folder_with_timestamp(folder_path, output_dir):
    # 生成当前时间字符串，格式为YYYY-MM-DD_HH-MM
    current_time = datetime.now().strftime("%Y-%m-%d_%H%M")
    zip_filename = f"{current_time}.zip"
    zip_path = os.path.join(output_dir, zip_filename)
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    # 使用shutil创建ZIP文件（简单高效）
    shutil.make_archive(
        base_name=os.path.join(output_dir, current_time),  # 不带扩展名的文件名
        format='zip',
        root_dir=folder_path,
    )
    
    print(f"文件夹 '{folder_path}' 已压缩为 '{zip_path}'")
    return zip_path

def copy_folder_and_rename(src_folder, new_name, target_dir=None):
    # 确定目标目录（默认为源文件夹的父目录）
    target_dir = target_dir or os.path.dirname(src_folder)
    # 构建目标文件夹完整路径
    dst_folder = os.path.join(target_dir, new_name).replace("/tempo","")
    try:
        # 如果目标文件夹已存在，则先删除[1](@ref)[7](@ref)
        if os.path.exists(dst_folder):
            shutil.rmtree(dst_folder)
            print(f"已删除已存在的目标文件夹: {dst_folder}")
        
        # 递归复制整个文件夹[2](@ref)[7](@ref)
        shutil.copytree(src_folder, dst_folder)
        print(f"文件夹 '{src_folder}' 已复制并重命名为 '{dst_folder}'")
        return dst_folder
    except Exception as e:
        print(f"操作失败: {e}")
        return None

				
def zhuyin_stay_same():
    global donlist_zhuyin
    donlist_zhuyin = [  t for t in code_list ]
    shutil.copy2(load_ds_path+'dict_zhuyin_caData.pkl', save_ds_path)
    shutil.copy2(load_ds_path+'combined.csv', save_ds_path)


def is_timestamp_column(series, threshold=0.9):
    s = series.dropna()
    if len(s) == 0:
        return False
    # 只保留数值类型（int 或 float）
    numeric_s = s.apply(lambda x: isinstance(x, (int, float)) and not isinstance(x, bool))
    s_numeric = s[numeric_s]
    
    if len(s_numeric) == 0:
        return False

    # 转为 float 或 int 数组
    values = pd.to_numeric(s_numeric, errors='coerce').dropna()
    if len(values) == 0:
        return False

    # 定义常见时间戳范围（毫秒级：2000-2030 年之间）
    # 毫秒级时间戳范围（2000-01-01 到 2030-01-01）
    min_ts_ms = 946684800000   # 2000-01-01 UTC
    max_ts_ms = 1893456000000 # 2030-01-01 UTC

    # 秒级时间戳范围
    min_ts_s = min_ts_ms / 1000
    max_ts_s = max_ts_ms / 1000

    # 判断是毫秒还是秒
    # 统计在毫秒范围内的比例
    in_ms_range = ((values >= min_ts_ms) & (values <= max_ts_ms)).mean()
    in_s_range = ((values >= min_ts_s) & (values <= max_ts_s)).mean()

    # 如果大部分值落在毫秒或秒的时间戳范围内，则认为是时间戳列
    return (in_ms_range > threshold) or (in_s_range > threshold)


# ======== 核心数据结构 ========
@dataclass
class DataSource:
    name: str
    check_condition: Callable[[], Coroutine]  # 定义异步条件检查函数
    download_task: Callable[[], Coroutine]    # 异步下载函数
    status: str = "pending"                    # 任务状态 (pending/running/completed)
    retry_count: int = 0                        #重试时间
    last_run: float = 0.0                     # 最后运行时间戳

# ======== 调度器实现 ========
class AsyncScheduler:
    def __init__(self, sources: list[DataSource], max_concurrent=1):
        self.sources = {src.name: src for src in sources}
        self.semaphore = asyncio.Semaphore(max_concurrent)  # 并发控制
        self.active_tasks = set()

    async def _process_source(self, source_name: str):
        """处理单个数据源的完整生命周期"""
        source = self.sources[source_name]
        
        # 条件检查阶段
        if not await source.check_condition():     ##if not true
            print(f"🚦 [{source.name}] 数据不完整，触发下载")
            source.status = "running"
            
            # 下载执行阶段（带并发控制）
            async with self.semaphore:
                try:
                    await source.download_task()
                    print(f"⬇️  [{source.name}] 下载完成")
                except Exception as e:
                    print(f"❌ [{source.name}] 下载失败: {str(e)}")
                    source.retry_count += 1
                    await asyncio.sleep(5)
                    if source.retry_count > 3:
                        print(f"⛔ [{source.name}] 超过最大重试次数，终止任务")
                        return
                    
            # 下载后重新验证
            if await source.check_condition():
                print(f"✅ [{source.name}] 验证通过，任务完成")
                source.status = "completed"
            else:
                print(f"🔄 [{source.name}] 验证未通过，重新加入检查队列")
                await self._process_source(source_name)  # 递归重试
        else:
            print(f"🏁 [{source.name}] 数据完整，直接完成")
            source.status = "completed"

    async def start(self, interval=60):
        """启动调度器主循环"""
        while True:
            print(f"\n===== 开始新一轮检查 [{time.strftime('%H:%M:%S')}] =====")
            # 并行触发所有条件检查
            tasks = [
                asyncio.create_task(self._process_source(name))
                for name in self.sources.keys()
            ]
            await asyncio.gather(*tasks)
            
            # 检查所有任务是否完成
            if all(src.status == "completed" for src in self.sources.values()):
                print("🌟 所有数据源已完成处理，调度终止")
                break
                
            print(f"⏱️ 下次检查将在 {interval} 秒后...")
            await asyncio.sleep(interval)

# ======== 下载任务定义 ======== 🚒
async def check_price_data_state(  ):
    ##check is stock_list items in  code_list if is continue
    ##check is stock_list items in  data_priec
    ## if in is the date is newest ,if TRUE add into the donlist_p, else retun True
    #for code_ in  tqdm(code_list, total=len(code_list), desc="check_price_data_state"):
        #if code_ in donlist_p:continue
        #if code_ in data_priec.keys()
            #data_priec[ code_].日期.max()
    res_= [t for t in code_list if t not in donlist_p] 
    print( "len check_price_data_state",str(   len(res_) )  )
    if pc_count>50:
    	print( " pc_count >50 now return true for skip"  )
    	print("### the res cannot update is:  ####")
    	print(   res_  )
    	return True
    if len(res_)>0:return False
    return True
async def loadng_price_data(    ):
    global donlist_p
    global pc_count
    pc_count+=1
    for code_ in  tqdm(code_list, total=len(code_list), desc="downloding price data "):
        market=identify_exchange(code_)
        if code_ in donlist_p:continue
        if code_ not in data_priec.keys() or  len(data_priec[ code_])==0:
            tp1_ = ts.pro_bar(ts_code=code_+'.'+market ,adj= None ,start_date='20170101',end_date=today_, factors=['tor' ])
            data_priec[code_] = pre_process_ak2ts( tp1_ ).sort_values(by="日期").reset_index(drop=True)
            data_priec[code_] ['日期'] = pd.to_datetime( data_priec[code_] ['日期']  )
        else:
            data_priec[code_]['日期'] = pd.to_datetime( data_priec[code_]['日期']  )
            next_day_ = data_priec[ code_].日期.max()  + timedelta(days=1)
            next_day_ = next_day_.strftime("%Y%m%d")
            if next_day_ == today_:continue
            tp1_ = ts.pro_bar(ts_code=code_+'.'+market ,adj= None ,start_date=next_day_,end_date=today_, factors=['tor' ])
            if len(tp1_)==0:
            	print("code_ is len ==0 ",  code_)
            	continue
            tp1_ = pre_process_ak2ts( tp1_ ).sort_values(by="日期")
            tp1_['日期'] = pd.to_datetime( tp1_['日期']  )
            data_priec[code_] =pd.concat(  [data_priec[code_] ,tp1_ ] ) 
            #data_priec[code_]['日期'] = pd.to_datetime( data_priec[code_]['日期']  , format='%Y%m%d')
            data_priec[code_]= data_priec[code_].sort_values(by="日期").drop_duplicates( subset=['日期'] , keep = 'first').reset_index(drop=True)
        donlist_p+=[code_]
        #tp_counter += 1 
    print( "the donlist_p",len(donlist_p)   )
    with open(save_ds_path+'data_price_wfq.pkl', 'wb') as f:pickle.dump(data_priec, f)  


async def check_pepb_data_state(  ):
    res_= [t for t in code_list if t not in donlist_pepb] 
    print( "len check_pepb_data_state",str(   len(res_) )  )
    if pb_count>50:
    	print( "pb_count >50 now return true for skip"  )
    	print("### the res cannot update is:  ####")
    	print(   res_  )
    	return True
    if len(res_)>0:return False
    return True
    
async def loadng_pepb_data(    ):
    def toDatatime( k_  ):
        if datapepb2[k_].trade_date.dtype !='<M8[ns]':
            if is_timestamp_column( datapepb2[k_].tail(20)['trade_date']  ):
                datapepb2[k_]['trade_date'] = pd.to_datetime(datapepb2[k_]['trade_date'] , unit='ms')
            datapepb2[k_]['trade_date'] = pd.to_datetime(datapepb2[k_]['trade_date'] ,  format='%Y%m%d')
    global donlist_pepb
    global pb_count
    pb_count+=1
    for code_ in  tqdm(code_list, total=len(code_list), desc="downloding pepb data "):
        market=identify_exchange(code_)
        if code_ in donlist_pepb:continue
        if code_ not in datapepb2.keys() or  len(datapepb2[ code_])==0:
            tp1_ = pro.daily_basic(ts_code=  code_+"."+market , start_date='20170101',end_date=today_ )
            if len(tp1_)>0:tp1_['trade_date']  = pd.to_datetime(tp1_['trade_date'] ,  format='%Y%m%d')
            datapepb2[code_] =tp1_ 
        else:
            #if len(  datapepb2[ code_]  )==0:continue # v1.1.3
            toDatatime( code_ )
            next_day_ = datetime.strptime(datapepb2[ code_].trade_date.max().strftime( '%Y%m%d' ), "%Y%m%d")
            next_day_ =next_day_ + timedelta(days=1) 
            next_day_ = next_day_.strftime("%Y%m%d")
            if next_day_ == today_:
                donlist_pepb+=[code_] # v1.1.3
                continue 
            tp1_ = pro.daily_basic(ts_code=  code_+"."+market , start_date=  next_day_,end_date=today_ )
            if len(tp1_)==0:    # v1.1.3
                print("code_ is len ==0 ",  code_)
                continue
            datapepb2[code_] =pd.concat(  [datapepb2[code_] ,tp1_ ] )
            datapepb2[code_]['trade_date'] = pd.to_datetime( datapepb2[code_]['trade_date']  , format='%Y%m%d')
            datapepb2[code_]= datapepb2[code_].sort_values(by="trade_date").drop_duplicates( subset=['trade_date'] , keep = 'first').reset_index(drop=True)
        donlist_pepb+=[code_]

    #save_dict(  copy.deepcopy(  datapepb2 ), 'dict_pepb.json')
    print( "the donlist_pepb",len(donlist_pepb)   )
    with open(save_ds_path+'dict_pepb.pkl', 'wb') as f:pickle.dump(datapepb2, f)


async def check_fenghong_state(  ):
    res_= [t for t in code_list if t not in donlist_fh  ] 
    print( "len check_fenghong_state",str(   len(res_) )  )
    if fh_count>50:
    	print( "fh_count >50 now return true for skip"  )
    	print("### the res cannot update is:  ####")
    	print(   res_  )
    	return True
    if len(res_)>0:return False
    return True
    
async def loadng_fenghong_data(  ):
    global donlist_fh
    global fh_count
    fh_count+=1
    for idx, code in tqdm(enumerate(code_list), total=len(code_list), desc="downloding fenhong "   ):
        if code in donlist_fh:continue
        market=identify_exchange(code)
        df = pro.dividend(ts_code=  code+"."+market ) 
        fenhong_dict_[code] =df 
        donlist_fh+=[code]
        #tp_counter+=1
    #save_dict(  copy.deepcopy(  fenhong_dict_ ), 'dict_fenhong.json') 
    print( "the donlist_fh",len(donlist_fh)   )
    with open(save_ds_path+'fenghong_dict.pkl', 'wb') as f:pickle.dump(fenhong_dict_, f)

async def check_zhuyin_state(  ):
    res_= [t for t in code_list if t not in donlist_zhuyin  ] 
    print( "len check_zhuyin_state",str(   len(res_) )  )
    if zz_count>50:
    	print( "zz_count >50 now return true for skip"  )
    	print("### the res cannot update is:  ####")
    	print(   res_  )
    	return True
    if len(res_)>0:return False
    return True
async def loadng_zhuyin_data(  ):
    global donlist_zhuyin
    global zz_count
    zz_count+=1
    for idx, code in tqdm(enumerate(code_list), total=len(code_list)   , desc="downlidng zhuyin"  ):
        if code not in donlist_zhuyin:
            market=identify_exchange(code)
            df = pro.fina_mainbz(ts_code=  code+"."+market, type='I' )
            if len( df )==0:continue 
            if code in zhuyin_dict_.keys():
                df = pd.concat([zhuyin_dict_[ code ] ,  df   ]  , axis =0  )
                df['end_date'] = df.end_date.astype(int) #v1.1.3  避免重复
            df= df.drop_duplicates(subset=['end_date','bz_item'], keep = 'last' )
            df= df.reset_index(drop=True)
            zhuyin_dict_[code] =  df
        donlist_zhuyin+=[code]
    combined_df = pd.concat(zhuyin_dict_.values(), ignore_index=True)
    combined_df= combined_df[combined_df.bz_code =='I']
    combined_df[ 'bz_sales_pct']= combined_df.groupby(['ts_code','end_date'])['bz_sales'].transform('sum')
    combined_df[ 'bz_sales_pct']  =  combined_df[ 'bz_sales'] /combined_df[ 'bz_sales_pct'] 
    combined_df.to_csv(save_ds_path+'combined.csv', index=False) 
    #save_dict(  copy.deepcopy(  zhuyin_dict_ ), 'dict_zhuyin_caData.json')
    print( "the donlist_zhuyin",len(donlist_zhuyin)   )
    with open(save_ds_path+'dict_zhuyin_caData.pkl', 'wb') as f:pickle.dump(zhuyin_dict_, f)
    

# ---------------------------------------------------------
# v1.1.0 - 2025-09-22
# Added: adding  add checkname data, check state of the checkname data
async def check_name_space_state( ):
	res_= [t for t in code_list+ts_code_list if t not in donlist_ns  ]
	print( "len check_name_space_state  ",str(   len(res_) )  ) 
	if ns_count>20:#v1.1.1 - 2025-09-24
		print( "ns_count >20 now return true for skip"  ) #v1.1.1 - 2025-09-24
		print("### the res cannot update is:  ####")#v1.1.1 - 2025-09-24
		print(   res_  ) #v1.1.1 - 2025-09-24
	if len(res_)>0:return False
	return True

async def loadng_namespace_data(  ):
	global ns_count
	global donlist_ns
	ns_count+=1
	for idx, code in tqdm(enumerate(code_list+ts_code_list ), total=len(code_list+ts_code_list), desc="downloding namespace "   ):
		if code in donlist_ns:continue
		market = identify_exchange(code)
		df = pro.namechange(ts_code=  code+"."+market  , fields='ts_code,name,start_date,end_date,ann_date,change_reason')#v1.1.2 
		namespace_dict[code] =df #v1.1.1 - 2025-09-24 namespace_dict_ to namespace_dict
		donlist_ns+=[code]
	print( "the donlist_ns",len(donlist_ns)   )
	with open(save_ds_path+'namespace_dict.pkl', 'wb') as f:pickle.dump(namespace_dict, f) #v1.1.1 - 2025-09-24
# ---------------------------------------------------------   

def update_zy_mp_zzfj_dict_qwen():
    combined_df = pd.read_csv(save_ds_path+'combined.csv') 
    infer_ind_list =list(  [ it_ for it_ in combined_df[(combined_df.end_date.astype(int) >20150000)&(  combined_df.end_date.astype(str).str.contains('1231')  )       &( combined_df.bz_sales_pct >0.2 ) ].bz_item.unique().tolist() if it_ not in zhuyin_hangye_dict.keys()            ])    
    #infer_ind_list= infer_ind_list[:55]
    output_txt=''
    output_dict = {} 
    industry_typ_l3= ['油气开采与油田服务',  '石油与天然气', '煤炭', '农用化工', '化学纤维', '化学原料', '化学制品', '塑料', '橡胶', '工业金属', '贵金属', '稀有金属', '其他有色金属及合金', '钢铁', '建筑材料', '其他非金属材料', '容器与包装', '纸类与林业产品', '航空航天', '国防装备', '建筑与工程', '建筑装修', '建筑产品', '发电设备', '电网设备', '储能设备', '通用机械', '专用机械', '交通运输设备', '工业集团企业', '污染治理', '节能与生态修复', '商业服务与用品', '运输业', '交通基本设施', '汽车零部件与轮胎', '乘用车', '摩托车及其他', '汽车经销商与汽车服务', '家用电器', '家居', '休闲设备与用品', '纺织服装', '珠宝与奢侈品', '休闲服务', '教育服务', '其他消费者服务', '一般零售', '专营零售', '旅游零售', '互联网零售', '酒', '软饮料', '食品', '烟草', '种植', '养殖', '家庭用品', '美容护理', '医疗器械', '医疗商业与服务', '生物药品', '化学药', '中药', '制药与生物科技服务', '商业银行', '抵押信贷机构', '其他金融服务', '消费信贷', '证券公司', '其他资本市场', '保险', '软件开发', '信息技术服务', '电子终端及组件', '电子元件', '光学光电子', '电子化学品', '其他电子', '集成电路', '分立器件', '半导体材料与设备', '电信运营服务', '电信增值服务', '通信设备', '数据中心', '通信技术服务', '营销与广告', '文化娱乐', '数字媒体', '电力及电网', '燃气', '水务', '市政环卫', '供热及其他', '房地产开发与园区', '房地产管理与服务', '房地产投资信托(REITs)']
    exp_={
    '橡胶制品业':'橡胶制品',
    '运营(收入)':'null',
    '建造收入(行业)':'房屋建设',
     '机电工具制造(行业)':'其他通用机械',
     '信息系统集成服务':'系统集成服务'
    }
    exp_1=['橡胶制品业', '运营(收入)', '建造收入(行业)', '机电工具制造(行业)', '信息系统集成服务']
    for i_ in range(0,len(infer_ind_list ), 45):
        right_ = np.min( [ len(infer_ind_list )  ,i_ +45 ]  )
        query = infer_ind_list[i_:right_] 
        messages_ = [
        {"role": "system", "content": "你是一个办公室的文员"},
        {"role": "user", "content": "你的工作是对收集获得的'公司业务类型'，归类为设定的'标准分类'，并所以json格式输出，以下是所有的标准分类："},
        {"role": "user", "content": str(industry_typ_l3) },
        {"role": "user", "content": '以下是一个归类案例, '+str(exp_1)+'+'+'输出: '+str(exp_)  },
        {"role": "user", "content": "##下面是你需要处理的文本，请处理后以json输出，不要输出其他信息##"  },
        {"role": "user", "content": str(query )}
        ]
        #print(" ### ")
        #print(messages)
        #print(" ### ")
        t = time.time()
        client = OpenAI(
        # 若没有配置环境变量，请用百炼API Key将下行替换为：api_key="sk-xxx",
        api_key= 'sk-*******',   ##upload by yours
        base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        )
        completion = client.chat.completions.create(
        model="qwen-plus", # 模型列表：https://help.aliyun.com/zh/model-studio/getting-started/models
        messages=messages_ ,
         max_tokens= 1024,
         temperature= 0.25 ,
        )
        response = completion.choices[0].message.content   
        output_txt+=response
        if( is_valid_json(response)):output_dict.update( json.loads(response)  )
        else:output_dict.update( robust_json_extract(response)  )  
        print(     )
        print(  response   )
        print(time.time() -t  )  
        print(      )
        print(     '  %%%%%%%%%%%%%%%%%%%  ' )
        if i_%180 ==0:
            with open(f'output_dict.json', 'w') as file: json.dump(output_dict, file)
            #with open('output_txt.txt', 'w', encoding='utf-8') as f:f.write(output_txt)
            with open(save_ds_path+'hy_map_zhongzheng_add.pkl', 'wb') as f:pickle.dump(output_dict, f)
            with open(save_ds_path+'hy_map_zhongzheng.txt', 'w', encoding='utf-8') as f:f.write(output_txt)	
      
        with open(f'output_dict.json', 'w') as file: json.dump(output_dict, file)
        #with open('output_txt.txt', 'w', encoding='utf-8') as f:f.write(output_txt)
        with open(save_ds_path+'hy_map_zhongzheng_add.pkl', 'wb') as f:pickle.dump(output_dict, f)
        with open(save_ds_path+'hy_map_zhongzheng.txt', 'w', encoding='utf-8') as f:f.write(output_txt)	
        global zhuyin_hangye_dict
        zhuyin_hangye_dict.update( output_dict)
        with open(save_ds_path+f'hy_map_zhongzheng.pkl', 'wb') as file:pickle.dump(zhuyin_hangye_dict, file)			



# ======== 初始化与启动 ========
if __name__ == "__main__":
    stock_list = ak.stock_info_a_code_name()
    code_list = stock_list.code.tolist()#[:20]
    data_ts = pro.query('stock_basic', exchange='', list_status='D'  ,fields='ts_code,symbol,name,area,industry,list_date,delist_date') # v1.1.0 - 2025-09-22
    ts_code_list = [t[:6] for t in data_ts.ts_code.tolist()] # v1.1.0 - 2025-09-22
    data_ts.to_pickle(save_ds_path+'ts_data.pkl') # v1.1.0 - 2025-09-22
    today_ = datetime.now().strftime("%Y%m%d")
    print(f'the updating day {today_}' )  # 输出示例：20250613
    with open(load_ds_path+'data_price_wfq.pkl', 'rb') as f:data_priec = pickle.load(f)
    with open(load_ds_path+ 'dict_pepb.pkl', 'rb') as file:datapepb2 = pickle.load(file)
    #for k in datapepb2.keys():datapepb2[k] =  pd.read_json(datapepb2[k]) 
    with open(load_ds_path+'fenghong_dict.pkl', 'rb') as file:fenhong_dict_ = pickle.load(file)
    if isUpadateFh==False:donlist_fh= [  t for t in code_list ]
    #zhuyin_dict_ = pickle.load(load_ds_path+ 'dict_zhuyin_caData.pkl') 
    with open(load_ds_path+'dict_zhuyin_caData.pkl', 'rb') as file:zhuyin_dict_ = pickle.load(file)
    with open(load_ds_path+'namespace_dict.pkl', 'rb') as file:namespace_dict = pickle.load(file) # v1.1.0 - 2025-09-22  old namesapce for update
    if isUpadateZhuyin==False:
    	zhuyin_stay_same( )  
    # 配置数据源（A-D）
    sources = [
        DataSource(name="price",  #任务名 
                  check_condition=check_price_data_state,#检查函数 
                  download_task=loadng_price_data) #执行函数
        ,DataSource(name="pepb",  check_condition=check_pepb_data_state,    download_task=loadng_pepb_data ),
         DataSource(name="fenhong",  check_condition= check_fenghong_state,    download_task= loadng_fenghong_data )
        ,DataSource(name="zhuyin",  check_condition= check_zhuyin_state,    download_task= loadng_zhuyin_data )
        ,DataSource(name="nameSpace",  check_condition= check_name_space_state,    download_task= loadng_namespace_data ) # v1.1.0 - 2025-09-22
    ]
    # 创建并启动调度器
    scheduler = AsyncScheduler(sources, max_concurrent=5 ) # v1.1.1 - 2025-09-24  [change]:max_concurrent=4 → max_concurrent=5
    asyncio.run( scheduler.start(interval=40)   ) ##任务开始 interval = 耗时  
    print("🗜 开始Qwen推理")
    with open(load_ds_path+'hy_map_zhongzheng.pkl', 'rb') as file:zhuyin_hangye_dict = pickle.load(file)
    update_zy_mp_zzfj_dict_qwen()
    print("🚩 完成Qwen推理")
    print("🗜 现在进行备份为zip")
    zip_folder_with_timestamp(save_ds_path, backup_ds_path)
    print("🗜 现在把下载的数据存为最新数据")
    copy_folder_and_rename(save_ds_path, 'newest2')
