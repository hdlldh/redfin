# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sqlite3
import datetime as dt
import os

class RedfinPipeline(object):
    def process_item(self, item, spider):
        return item


class SQLiteStoreItemPipeline(object):

    def __init__(self):

        self.EventDate = dt.datetime.now().strftime('%Y-%m-%d')
        cur_path, _ = os.path.split(__file__)
        root_path = os.path.dirname(cur_path)
        self.file_name = os.path.join(root_path, 'redfin/data/redfin_home_%s.db' % (dt.datetime.now().strftime('%Y%m%d')))
        print(self.file_name)
        self.conn = sqlite3.connect(self.file_name)


    def create_table(self, table_name, keys):
        try:
            query = """CREATE TABLE IF NOT EXISTS %s (%s, Primary Key (URL, EventDate))
                """ % (table_name, ', '.join(['%s text' % k for k in keys]))
            self.conn.execute(query)
            self.conn.commit()
            print('Succeed to create table: %s'%table_name)
            return 0
        except Exception as e:
            print('Failed to create table: %s'%table_name)
            print(query)
            print("Error: %s"%type(e).__name__)
            return 1

    def insert_data(self, table_name, values):
        try:
            question_marks = ', '.join(['?'] * len(values))
            query = 'INSERT or REPLACE INTO %s VALUES (%s)' % (table_name, question_marks)
            cur = self.conn.cursor()
            cur.execute(query, values)
            self.conn.commit()
            print('Succeed to insert the item.')
            return 0
        except Exception as e:
            print('Failed to insert the item.')
            print(query)
            print(values)
            print("Error: %s" % type(e).__name__)
            return 1

    def process_item(self, item, spider):
        keys = []
        values = []
        for k, v in item.items():
            if 'URL' in k:
                keys.append('URL')
            else:
                keys.append('"%s"'%k)
            if 'CITY' in k or 'LOCATION' in k or 'ADDRESS' in k:
                values.append(v.title())
            else:
                values.append(v)
        table_name = spider.name.split('_')[1]
        rst = self.insert_data(table_name, values)
        if rst:
            self.create_table(table_name, keys)
            self.insert_data(table_name, values)

        return item
