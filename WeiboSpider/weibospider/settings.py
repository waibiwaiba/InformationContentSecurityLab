# -*- coding: utf-8 -*-
import os
BOT_NAME = 'spider'

SPIDER_MODULES = ['spiders']
NEWSPIDER_MODULE = 'spiders'

ROBOTSTXT_OBEY = False
# 获取当前文件所在目录的绝对路径
dir_path = os.path.dirname(os.path.abspath(__file__))
# print(dir_path)
# 构建 cookie.txt 的绝对路径
cookie_path = os.path.join(dir_path, "cookie.txt")

with open(cookie_path, 'rt', encoding='utf-8') as f:
    cookie = f.read().strip()
DEFAULT_REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:61.0) Gecko/20100101 Firefox/61.0',
    'Cookie': cookie
}

CONCURRENT_REQUESTS = 16

DOWNLOAD_DELAY = 1

DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': None,
    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': None,
    'middlewares.IPProxyMiddleware': 100,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 101,
}

ITEM_PIPELINES = {
    'pipelines.JsonWriterPipeline': 300,
}
