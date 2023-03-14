# -*- coding: utf-8 -*-
import datetime
import json
import os.path
import time


class JsonWriterPipeline(object):
    """
    写入json文件的pipeline
    """

    def __init__(self):
        # 获取当前文件所在目录的绝对路径
        self.dir_path = os.path.dirname(os.path.abspath(__file__))
        # 构建 output 的绝对路径
        self.output_path = os.path.join(self.dir_path, "output")
        self.file = None
        if not os.path.exists(self.output_path):
            os.mkdir(self.output_path)

    def process_item(self, item, spider):
        """
        处理item
        """
        if not self.file:
            now = datetime.datetime.now()
            file_name = spider.name + "_" + now.strftime("%Y%m%d%H%M%S") + '.jsonl'
            file_path = os.path.join(self.output_path, file_name)
            self.file = open(file_path, 'wt', encoding='utf-8')
        item['crawl_time'] = int(time.time())
        line = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(line)
        self.file.flush()
        return item
