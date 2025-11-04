import tushare as ts 
import pandas as pd
import os
from typing import Optional, Dict, List

# 尝试加载.env文件中的环境变量
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("已加载.env文件中的环境变量")
except ImportError:
    print("未安装python-dotenv，将使用系统环境变量")
    print("建议安装: pip install python-dotenv")

# 从环境变量获取tushare token
def get_tushare_token() -> str:
    """
    从环境变量获取tushare token
    
    返回:
        str: tushare token，如果未设置则返回空字符串
    """
    token = os.getenv('TUSHARE_TOKEN')
    if not token:
        print("警告: 未找到TUSHARE_TOKEN环境变量")
        print("请在.env文件中设置TUSHARE_TOKEN，或直接设置环境变量")
        print("示例: TUSHARE_TOKEN=your_token_here")
        return ""
    return token

# 初始化tushare pro接口
def init_tushare():
    """
    初始化tushare pro接口
    """
    token = get_tushare_token()
    if token:
        ts.set_token(token)
        print("已使用环境变量中的token初始化tushare")
    else:
        print("使用默认方式初始化tushare（需要手动设置token）")
    
    return ts.pro_api()

# 初始化pro接口
pro = init_tushare()

def get_stock_basic_info(exchange: str = '', list_status: str = 'L') -> pd.DataFrame:
    """
    获取股票基本信息，包括股票代码、名称、地区、行业等
    
    参数:
        exchange (str): 交易所代码，默认为空字符串表示所有交易所
        list_status (str): 上市状态，'L'表示上市，'D'表示退市，'P'表示暂停上市
    
    返回:
        pd.DataFrame: 包含股票基本信息的DataFrame
        列包括: ts_code, symbol, name, area, industry, list_date
    
    异常:
        当API调用失败时会抛出异常
    """
    try:
        # 查询当前所有正常上市交易的股票列表
        data = pro.stock_basic(
            exchange=exchange, 
            list_status=list_status, 
            fields='ts_code,symbol,name,area,industry,list_date'
        )
        
        # 检查是否成功获取数据
        if data is None or data.empty:
            print("警告: 未获取到任何股票数据")
            return pd.DataFrame()
        
        print(f"成功获取 {len(data)} 只股票的基本信息")
        return data
        
    except Exception as e:
        print(f"获取股票基本信息时发生错误: {e}")
        raise

def get_stock_info_by_code(ts_code: str) -> Optional[Dict]:
    """
    根据股票代码获取单只股票的详细信息
    
    参数:
        ts_code (str): 股票代码，格式如 '000001.SZ'
    
    返回:
        Dict: 包含股票信息的字典，如果未找到则返回None
    """
    try:
        # 获取单只股票的基本信息
        data = pro.stock_basic(
            ts_code=ts_code,
            fields='ts_code,symbol,name,area,industry,list_date'
        )
        
        if data is None or data.empty:
            print(f"未找到股票代码 {ts_code} 的信息")
            return None
        
        # 返回第一行数据作为字典
        return data.iloc[0].to_dict()
        
    except Exception as e:
        print(f"获取股票 {ts_code} 信息时发生错误: {e}")
        return None

def get_stocks_by_industry(industry: str) -> pd.DataFrame:
    """
    根据行业筛选股票
    
    参数:
        industry (str): 行业名称，如 '银行', '房地产' 等
    
    返回:
        pd.DataFrame: 该行业的所有股票信息
    """
    try:
        # 获取所有股票信息
        all_stocks = get_stock_basic_info()
        
        if all_stocks.empty:
            return pd.DataFrame()
        
        # 筛选指定行业的股票
        industry_stocks = all_stocks[all_stocks['industry'] == industry]
        
        print(f"找到 {len(industry_stocks)} 只 {industry} 行业的股票")
        return industry_stocks
        
    except Exception as e:
        print(f"筛选 {industry} 行业股票时发生错误: {e}")
        return pd.DataFrame()

def get_stocks_by_area(area: str) -> pd.DataFrame:
    """
    根据地区筛选股票
    
    参数:
        area (str): 地区名称，如 '深圳', '北京' 等
    
    返回:
        pd.DataFrame: 该地区的所有股票信息
    """
    try:
        # 获取所有股票信息
        all_stocks = get_stock_basic_info()
        
        if all_stocks.empty:
            return pd.DataFrame()
        
        # 筛选指定地区的股票
        area_stocks = all_stocks[all_stocks['area'] == area]
        
        print(f"找到 {len(area_stocks)} 只 {area} 地区的股票")
        return area_stocks
        
    except Exception as e:
        print(f"筛选 {area} 地区股票时发生错误: {e}")
        return pd.DataFrame()

# 示例使用
if __name__ == "__main__":
    # 获取所有股票基本信息
    print("=== 获取所有股票基本信息 ===")
    all_stocks = get_stock_basic_info()
    if not all_stocks.empty:
        print(f"总共获取到 {len(all_stocks)} 只股票")
        print("\n前5只股票信息:")
        print(all_stocks.head())
    
    # 获取特定股票信息
    print("\n=== 获取特定股票信息 ===")
    stock_info = get_stock_info_by_code('000001.SZ')
    if stock_info:
        print("平安银行信息:")
        for key, value in stock_info.items():
            print(f"  {key}: {value}")
    
    # 获取银行行业股票
    print("\n=== 获取银行行业股票 ===")
    bank_stocks = get_stocks_by_industry('银行')
    if not bank_stocks.empty:
        print(f"银行行业股票数量: {len(bank_stocks)}")
        print(bank_stocks[['ts_code', 'name', 'area']].head())