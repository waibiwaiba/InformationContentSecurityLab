import sqlite3
import json
import os

# 设置文件路径
jsonl_file = 'search_spider_20230315075511.jsonl'
db_file = 'weibo_data.db'
jsonl_file_path = os.path.join(os.path.dirname(__file__), 'output', jsonl_file)
db_file_path = os.path.join(os.path.dirname(__file__), db_file)

# 1. 连接数据库
conn = sqlite3.connect(db_file_path)

# 2. 创建表
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS weibo (
                    id INTEGER PRIMARY KEY,
                    mblogid TEXT,
                    created_at TEXT,
                    geo TEXT,
                    ip_location TEXT,
                    reposts_count INTEGER,
                    comments_count INTEGER,
                    attitudes_count INTEGER,
                    source TEXT,
                    content TEXT,
                    pic_urls TEXT,
                    pic_num INTEGER,
                    isLongText INTEGER,
                    user_id TEXT,
                    user_avatar_hd TEXT,
                    user_nick_name TEXT,
                    user_verified INTEGER,
                    user_mbrank INTEGER,
                    user_mbtype INTEGER,
                    user_verified_type INTEGER,
                    video,
                    url TEXT,
                    keyword TEXT,
                    crawl_time INTEGER
                )''')
conn.commit()

# 3. 读取jsonl文件中的数据，将每行数据转换为Python字典，并插入数据库

with open(jsonl_file_path, 'r', encoding='utf-8') as f:
    count = 0
    added = 0
    duplicate = 0
    old = 0
    for line in f:
        count += 1

        data = json.loads(line)
        pic_urls = json.dumps(data.get('pic_urls'), ensure_ascii=False)
        user = data.get('user')
         # 查询该条微博是否已经在表中存在
        select_sql = "SELECT * FROM weibo WHERE id=?"
        id = data.get('_id')
        cursor.execute(select_sql, (id,))
        result = cursor.fetchone()

        # 如果查询结果为空，则插入该条数据
        if not result:
            geo =  data.get('geo')
            # 如果数据的geo项为空，则插入该条数据（旧帖子geo不为空，麻烦，就不加进数据库了。）
            if not geo:
                # 构造插入数据的元组
                values = (
                data.get('_id'),
                data.get('mblogid'),
                data.get('created_at'),
                data.get('geo'),
                data.get('ip_location'),
                data.get('reposts_count'),
                data.get('comments_count'),
                data.get('attitudes_count'),
                data.get('source'),
                data.get('content'),
                pic_urls,
                data.get('pic_num'),
                int(data.get('isLongText')),
                user.get('_id'),
                user.get('avatar_hd'),
                user.get('nick_name'),
                int(user.get('verified')),
                user.get('mbrank'),
                user.get('mbtype'),
                user.get('verified_type'),
                data.get('video'),
                data.get('url'),
                data.get('keyword'),
                data.get('crawl_time'))
                # 将values里的 None 元素用''替换，才能插入数据库
                values = tuple('' if v is None else v for v in values)
                # 构造 SQL 插入语句
                insert_sql = f"INSERT INTO weibo VALUES {values}"
                # print(insert_sql)
                # 执行 SQL 插入语句
                cursor.execute(insert_sql)
                # 提交更改
                conn.commit()

                added += 1
                print(f"插入id为{id}的数据成功！")
            else:
                old += 1
                print(f"id为{id}的数据格式很旧，跳过插入操作。")
        else:
            duplicate += 1
            print(f"id为{id}的数据已经存在，跳过插入操作。")
        conn.commit()
    # 打印转换结果
    print(f'读取的jsonl文件中的数据共有：{count}条')
    print(f'与数据库中已有的数据项重复的有：{duplicate}条')
    print(f'格式过旧的数据有：{old}条')
    print(f'新插入的数据项有：{added}条')

# 4. 关闭数据库连接
conn.close()
