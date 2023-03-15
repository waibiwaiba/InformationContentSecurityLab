#!/usr/bin/env python
# encoding: utf-8
"""
Author: rightyonghu
Created Time: 2022/10/22
"""
import json
import re
import requests
from bs4 import BeautifulSoup
from scrapy import Spider, Request
from spiders.common import parse_tweet_info, parse_long_tweet
from datetime import datetime, timedelta
from urllib.parse import quote


class SearchSpider(Spider):
    """
    关键词搜索采集
    """

    name = "search_spider"
    base_url = "https://s.weibo.com/"

    def find_hot_topics(self):
        """
        寻找当日的热点话题
        """
        keywords = []
        url = "https://weibo.cn/pub/"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        links = soup.find_all("a")
        pattern = r"#[^#]*#"
        for link in links:
            # print(link.text)
            matches = re.findall(pattern, link.text)
            if matches:
                for str in matches:
                    keywords.append(str)
        # keywords = ['#终于知道为什么老说猴急了#', '#阿根廷国家队关于中国行声明#']
        return keywords

    def search_time_scope(self, days_ago):
        """
        设置搜索时间区间：从当前时间到 days_ago 天之前
        """
        # 获取当前时间
        current_time = datetime.now()
        # 获取当前时间并格式化为指定格式
        current_time_str = datetime.now().strftime("%Y-%m-%d-%H")
        # 计算x天前的时间
        days_ago = days_ago
        previous_time = current_time - timedelta(days=days_ago)
        # 格式化时间为指定格式
        previous_time_str = previous_time.strftime("%Y-%m-%d-%H")
        # 格式为 年-月-日-小时, 2022-10-01-0 表示2022年10月1日0时
        return current_time_str, previous_time_str

    def start_requests(self):
        """
        爬虫入口
        """
        # 获取当日的热点话题
        keywords = self.find_hot_topics()
        # 默认搜索时间区间是当前时间到三天前
        days_ago = 3
        start_time, end_time = self.search_time_scope(days_ago)
        # 是否在指定的时间区间进行推文搜索
        is_search_with_specific_time_scope = False
        # 是否按照热度排序,默认按照时间排序
        is_sort_by_hot = True
        for keyword in keywords:
            print(keyword)  
            url = f"https://s.weibo.com/weibo?q={keyword}"
            if is_sort_by_hot:
                url += "&xsort=hot"
            if is_search_with_specific_time_scope:
                url += f"&timescope=custom%3A{end_time}%3A{start_time}&page=1"
            else:
                url += f"&page=1"
            escaped_url = quote(url, safe=':/?=&')
            print(url)
            print(escaped_url)
            yield Request(escaped_url, callback=self.parse, meta={"keyword": keyword})

    def parse(self, response, **kwargs):
        """
        网页解析
        """
        html = response.text
        tweet_ids = re.findall(r'\d+/(.*?)\?refer_flag=1001030103_" ', html)
        for tweet_id in tweet_ids:
            url = f"https://weibo.com/ajax/statuses/show?id={tweet_id}"
            yield Request(url, callback=self.parse_tweet, meta=response.meta)
        next_page = re.search('<a href="(.*?)" class="next">下一页</a>', html)
        if next_page:
            url = "https://s.weibo.com" + next_page.group(1)
            yield Request(url, callback=self.parse, meta=response.meta)

    @staticmethod
    def parse_tweet(response):
        """
        解析推文
        """
        data = json.loads(response.text)
        # print(data)
        # ok==0 代表该微博不存在
        if data["ok"] == 1:
            item = parse_tweet_info(data)
            item["keyword"] = response.meta["keyword"]
            if item["isLongText"]:
                url = "https://weibo.com/ajax/statuses/longtext?id=" + item["mblogid"]
                yield Request(url, callback=parse_long_tweet, meta={"item": item})
            else:
                yield item
