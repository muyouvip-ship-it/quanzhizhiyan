"""
量化课堂数据下载模块
API文档: https://www.quantclass.cn
"""

import os
import re
import zipfile
import tarfile
import logging
from typing import Optional, Dict, Any
import pandas as pd
import requests
from retrying import retry

logger = logging.getLogger(__name__)


class QuantClassDownloader:
    """量化课堂数据下载器"""
    
    def __init__(self, api_key: str, hid: str):
        self.api_key = api_key
        self.hid = hid
        self.base_url = 'https://api.quantclass.cn/api/data/'
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'content-type': 'application/json',
            'api-key': api_key
        }
    
    @retry(stop_max_attempt_number=5, wait_fixed=2000)
    def request_data(self, method: str, url: str, **kwargs) -> requests.Response:
        """请求数据"""
        res = requests.request(method=method, url=url, headers=self.headers, **kwargs)
        
        if res.status_code == 200:
            return res
        elif res.status_code == 404:
            logger.error('参数错误')
            raise Exception('参数错误')
        elif res.status_code == 403:
            logger.error('无下载权限，请检查下载次数与api-key')
            raise Exception('无下载权限')
        elif res.status_code == 401:
            logger.error('超出当日下载次数')
            raise Exception('超出当日下载次数')
        elif res.status_code == 400:
            logger.error('下载时间超出限制')
            raise Exception('下载时间超出限制')
        elif res.status_code == 500:
            logger.error('服务器内部错误')
            raise Exception('服务器内部错误')
        else:
            logger.error('获取数据失败')
            raise Exception('获取数据失败')
    
    def get_latest_time(self, product: str) -> str:
        """获取最新数据时间"""
        url = f'{self.base_url}/fetch/{product}-daily/latest?uuid={self.hid}'
        res = self.request_data('GET', url=url)
        
        if res.status_code == 200 and res.text:
            date_time_list = res.text.split(',')
            date_time = pd.DataFrame(date_time_list)[0].max()
            logger.info(f"产品 {product} 最新数据时间: {date_time}")
            return date_time
        else:
            raise Exception('获取最新时间失败')
    
    def get_download_link(self, product: str, date_time: str) -> str:
        """获取下载链接"""
        url = f'{self.base_url}/get-download-link/{product}-daily/{date_time}?uuid={self.hid}'
        res = self.request_data('GET', url=url)
        
        if res.status_code == 200 and res.text:
            logger.info(f"获取到下载链接")
            return res.text
        else:
            raise Exception('获取下载链接失败')
    
    def download_file(self, file_url: str, save_path: str) -> str:
        """下载文件"""
        # 提取文件名（支持csv、zip、tar等格式）
        file_name = re.findall(r'/([^/]+\.(?:csv|zip|tar|rar|7z|gz))', file_url)
        if file_name:
            file_name = file_name[0]
        else:
            # 从URL参数中提取
            file_name = file_url.split('/')[-1].split('?')[0]
        
        file_full_path = os.path.join(save_path, file_name)
        
        logger.info(f"开始下载文件: {file_name}")
        res = self.request_data('GET', url=file_url, stream=True)
        
        with open(file_full_path, 'wb') as f:
            for chunk in res.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        logger.info(f"文件下载完成: {file_full_path}")
        return file_full_path
    
    def unzip_file(self, file_path: str, extract_to: str) -> str:
        """解压文件（如果是压缩文件）"""
        file_ext = file_path.split('.')[-1].lower()
        
        # 如果是CSV文件，直接返回文件路径
        if file_ext == 'csv':
            logger.info(f"CSV文件无需解压: {file_path}")
            return file_path
        
        # 压缩文件需要解压
        logger.info(f"开始解压文件: {file_path}")
        
        if file_ext == 'zip':
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
        elif file_ext in ['tar', 'gz']:
            with tarfile.open(file_path, 'r:*') as tar_ref:
                tar_ref.extractall(extract_to)
        else:
            logger.warning(f"不支持的压缩格式: {file_ext}，返回原文件")
            return file_path
        
        logger.info(f"解压完成: {extract_to}")
        
        # 删除压缩文件
        os.remove(file_path)
        logger.info(f"已删除压缩文件: {file_path}")
        
        return extract_to
    
    def download_product(self, product: str, date_time: Optional[str] = None, 
                         save_path: str = './data/quantclass') -> Dict[str, Any]:
        """
        下载指定数据产品
        
        Args:
            product: 产品代码（如 stock-trading-data）
            date_time: 数据时间，None则自动获取最新
            save_path: 保存路径
        
        Returns:
            {
                'success': bool,
                'data_path': str,  # 解压后的数据目录
                'date_time': str,
                'product': str
            }
        """
        try:
            # 创建保存目录
            os.makedirs(save_path, exist_ok=True)
            
            # 获取最新时间
            if not date_time:
                date_time = self.get_latest_time(product)
            
            logger.info(f"准备下载产品: {product}, 时间: {date_time}")
            
            # 获取下载链接
            file_url = self.get_download_link(product, date_time)
            
            # 下载文件
            file_path = self.download_file(file_url, save_path)
            
            # 解压文件
            extract_path = os.path.join(save_path, f"{product}_{date_time}")
            data_path = self.unzip_file(file_path, extract_path)
            
            return {
                'success': True,
                'data_path': data_path,
                'date_time': date_time,
                'product': product
            }
            
        except Exception as e:
            logger.error(f"下载产品 {product} 失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'product': product
            }
    
    def list_available_products(self) -> Dict[str, str]:
        """列出可用的数据产品"""
        return {
            # K线数据
            'stock-trading-data': '股票历史日线数据',
            'stock-trading-data-pro': '股票历史全息日线数据',
            'stock-1h-trading-data': '股票1小时K线数据',
            'stock-5m-close-price': '股票5分钟收盘价',
            'stock-15m-close-price': '股票15分钟收盘价',
            
            # 指数数据
            'stock-main-index-data': '主要指数历史日线数据',
            'stock-1h-index-data': '指数1小时K线数据',
            
            # 高级数据
            'stock-chip-distribution': '筹码分布市场数据',
            'stock-money-flow': '资金流数据',
            'stock-fin-pre-data-sina': '财务预处理数据',
            'stock-analyst-ranking': '分析师评级数据',
            'stock-lhb-organ': '龙虎榜机构持仓数据',
            
            # 其他数据
            'stock-dividend-delivery': '个股分红数据',
            'stock-trading-date': '每日A股股票汇总',
            'stock-notices-title': '股票公告标题汇总',
        }


# 使用示例
if __name__ == '__main__':
    # 配置
    API_KEY = os.getenv('QUANTCLASS_API_KEY', '')
    HID = os.getenv('QUANTCLASS_HID', '')
    
    # 创建下载器
    downloader = QuantClassDownloader(API_KEY, HID)
    
    # 列出可用产品
    products = downloader.list_available_products()
    print("可用数据产品:")
    for code, name in products.items():
        print(f"  {code}: {name}")
    
    # 下载测试：股票日线数据
    result = downloader.download_product('stock-trading-data')
    
    if result['success']:
        print(f"\n✅ 下载成功!")
        print(f"数据路径: {result['data_path']}")
        print(f"数据时间: {result['date_time']}")
    else:
        print(f"\n❌ 下载失败: {result.get('error')}")
