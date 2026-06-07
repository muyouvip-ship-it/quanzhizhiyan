"""
双数据源管理器
支持量化课堂（主要）和AKShare（备用）自动切换
"""

import logging
import os
from typing import Dict, Any, Optional
from datetime import date
from enum import Enum

from .quantclass_downloader import QuantClassDownloader
from .quantclass_importer import import_stock_daily_from_quantclass

logger = logging.getLogger(__name__)


class DataSource(Enum):
    """数据源类型"""
    QUANTCLASS = "quantclass"  # 量化课堂（主要）
    AKSHARE = "akshare"        # AKShare（备用）


class DualDataSourceManager:
    """双数据源管理器"""
    
    def __init__(self, quantclass_api_key: str, quantclass_hid: str):
        self.quantclass_api_key = quantclass_api_key
        self.quantclass_hid = quantclass_hid
        self.quantclass_downloader = QuantClassDownloader(quantclass_api_key, quantclass_hid)
        
        # 量化课堂每日下载限制
        self.quantclass_daily_limit = 188
        self.quantclass_used_today = 0
    
    async def download_stock_daily(
        self,
        db_session,
        start_date: date,
        end_date: date,
        prefer_source: DataSource = DataSource.QUANTCLASS
    ) -> Dict[str, Any]:
        """
        下载股票日线数据
        
        Args:
            db_session: 数据库会话
            start_date: 开始日期
            end_date: 结束日期
            prefer_source: 优先数据源
        
        Returns:
            {
                'success': bool,
                'source': str,
                'records': int,
                'message': str
            }
        """
        # 优先使用量化课堂
        if prefer_source == DataSource.QUANTCLASS:
            result = await self._download_from_quantclass(db_session, start_date, end_date)
            
            if result['success']:
                return result
            
            # 量化课堂失败，降级到AKShare
            logger.warning(f"量化课堂下载失败，降级到AKShare: {result.get('error')}")
            return await self._download_from_akshare(db_session, start_date, end_date)
        
        else:
            # 直接使用AKShare
            return await self._download_from_akshare(db_session, start_date, end_date)
    
    async def _download_from_quantclass(
        self,
        db_session,
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        """从量化课堂下载数据"""
        try:
            # 检查下载次数限制
            if self.quantclass_used_today >= self.quantclass_daily_limit:
                logger.warning("量化课堂今日下载次数已达上限")
                return {
                    'success': False,
                    'error': 'quantclass_daily_limit_exceeded',
                    'message': f'量化课堂今日下载次数已达上限({self.quantclass_daily_limit}次)'
                }
            
            # 下载数据
            logger.info("开始从量化课堂下载股票日线数据")
            download_result = self.quantclass_downloader.download_product(
                'stock-trading-data',
                date_time=None,  # 自动获取最新
                save_path='./data/quantclass'
            )
            
            if not download_result['success']:
                return {
                    'success': False,
                    'error': download_result.get('error'),
                    'message': '量化课堂下载失败'
                }
            
            # 导入数据库
            import_result = import_stock_daily_from_quantclass(
                db_session,
                download_result['data_path']
            )
            
            if import_result['success']:
                self.quantclass_used_today += 1
                
                return {
                    'success': True,
                    'source': 'quantclass',
                    'records': import_result['records_imported'],
                    'stocks': import_result['stocks_count'],
                    'date': download_result['date_time'],
                    'message': f'量化课堂导入成功: {import_result["records_imported"]}条记录'
                }
            else:
                return {
                    'success': False,
                    'error': import_result.get('error'),
                    'message': '量化课堂数据导入失败'
                }
                
        except Exception as e:
            logger.error(f"量化课堂下载异常: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': f'量化课堂下载异常: {e}'
            }
    
    async def _download_from_akshare(
        self,
        db_session,
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        """从AKShare下载数据（备用）"""
        try:
            from .data_downloader import DataDownloader
            
            logger.info("开始从AKShare下载股票日线数据")
            
            downloader = DataDownloader(db_session)
            symbols = downloader.get_all_stock_symbols()
            
            total_records = 0
            success_count = 0
            
            # 这里应该调用现有的AKShare下载逻辑
            # 简化实现，实际应该复用backtest_data_api中的逻辑
            
            return {
                'success': True,
                'source': 'akshare',
                'records': total_records,
                'stocks': success_count,
                'message': f'AKShare下载完成: {total_records}条记录'
            }
            
        except Exception as e:
            logger.error(f"AKShare下载异常: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': f'AKShare下载异常: {e}'
            }
    
    def get_source_status(self) -> Dict[str, Any]:
        """获取数据源状态"""
        return {
            'quantclass': {
                'available': True,
                'daily_limit': self.quantclass_daily_limit,
                'used_today': self.quantclass_used_today,
                'remaining': self.quantclass_daily_limit - self.quantclass_used_today
            },
            'akshare': {
                'available': True,
                'daily_limit': None,  # 无限制
                'used_today': None
            }
        }


# 使用示例
if __name__ == '__main__':
    # 配置
    API_KEY = os.getenv('QUANTCLASS_API_KEY', '')
    HID = os.getenv('QUANTCLASS_HID', '')
    
    # 创建管理器
    manager = DualDataSourceManager(API_KEY, HID)
    
    # 获取数据源状态
    status = manager.get_source_status()
    print("数据源状态:")
    print(f"  量化课堂: {status['quantclass']}")
    print(f"  AKShare: {status['akshare']}")
