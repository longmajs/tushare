# -*- coding:utf-8 -*- 
"""
connection for api 
Created on 2017/09/23
@author: Jimmy Liu
@group : waditu
@contact: jimmysoa@sina.cn
"""
from tushare.stock import cons as ct
import logging
LOG = logging.getLogger("tushare.conns")

try:
    from pytdx.hq import TdxHq_API
    from pytdx.exhq import TdxExHq_API
    _PYTDX_IMPORT_ERROR = None
except Exception as _exc:
    TdxHq_API = None
    TdxExHq_API = None
    _PYTDX_IMPORT_ERROR = _exc


def _ensure_pytdx():
    if TdxHq_API is None or TdxExHq_API is None:
        raise ImportError("pytdx is required for this API. Please install pytdx.") from _PYTDX_IMPORT_ERROR


def api(retry_count=3):
    _ensure_pytdx()
    for _ in range(retry_count):
        try:
            api = TdxHq_API(heartbeat=True)
            api.connect(ct._get_server(), ct.T_PORT)
        except Exception as e:
            LOG.warning("%s", e)
        else:
            return api
    raise IOError(ct.NETWORK_URL_ERROR_MSG)


def xapi(retry_count=3):
    _ensure_pytdx()
    for _ in range(retry_count):
        try:
            api = TdxExHq_API(heartbeat=True)
            api.connect(ct._get_xserver(), ct.X_PORT)
        except Exception as e:
            LOG.warning("%s", e)
        else:
            return api
    raise IOError(ct.NETWORK_URL_ERROR_MSG)


def xapi_x(retry_count=3):
    _ensure_pytdx()
    for _ in range(retry_count):
        try:
            api = TdxExHq_API(heartbeat=True)
            api.connect(ct._get_xxserver(), ct.X_PORT)
        except Exception as e:
            LOG.warning("%s", e)
        else:
            return api
    raise IOError(ct.NETWORK_URL_ERROR_MSG)


def get_apis():
    return api(), xapi()


def close_apis(conn):
    api, xapi = conn
    try:
        api.disconnect()
        xapi.disconnect()
    except Exception as e:
        LOG.warning("%s", e)
