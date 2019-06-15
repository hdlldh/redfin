# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
import datetime as dt
from io import BytesIO
import os

class RedfinSfHomeSpider(scrapy.Spider):
    name = 'redfin_sf_home'
    allowed_domains = ['redfin.com']

    def __init__(self):
        cur_path, _ = os.path.split(__file__)
        root_path = os.path.dirname(cur_path)
        ref_file = os.path.join(root_path,'region_id_map/redfin_sf_home.csv')
        df = pd.read_csv(ref_file, dtype=object)
        region_id_list = list(df['RegionId'])

        self.EventDate = dt.datetime.now().strftime('%Y-%m-%d')
        url_base = 'https://www.redfin.com/stingray/api/gis-csv?al=1&market=sanfrancisco&num_homes=350&ord=redfin-recommended-asc&page_number=1&region_id=%s&region_type=6&sf=1,2,3,5,6,7&sp=true&status=9&uipt=1,2,3,4,5,6&v=8'
        self.start_urls = [url_base%region_id for region_id in region_id_list]


    def parse(self, response):
        df = pd.read_csv(BytesIO(response.body),dtype=object)
        df.fillna('',inplace=True)
        for _, row in df.iterrows():
            item = dict(row)
            if item:
                for k, v in item.items():
                    item[k] = str(v).strip('"')

                item['EventDate'] = self.EventDate
                item['Timestamp'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                yield item
            else:
                yield None




