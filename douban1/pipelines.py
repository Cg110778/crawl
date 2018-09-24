# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from douban1.items import *

class MongoPipeline(object):
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.db[DoubanuserItem.collection].create_index([('id', pymongo.ASCENDING)])

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        #name = item.__class__.__name__
        #self.db[name].insert(dict(item))
        if isinstance(item, DoubanuserItem):
            self.db[item.collection].update({'id': item.get('id')}, {'$set': item}, True)
        if isinstance(item, UserRelationItem):
            self.db[item.collection].update(
                {'id': item.get('id')},
                {'$addToSet':
                    {
                        'contacts': {'$each': item['contacts']},
                        #'rev_contacts': {'$each': item['rev_contacts']}
                    }
                }, True)
        if isinstance(item, DoubandetailmovieItem):
            self.db[item.collection].update({'movie_id': item.get('movie_id')}, {'$set': item}, True)
        if isinstance(item, DoubandetailmoviecommentItem) :
            self.db[item.collection].update(
                {'movie_id': item.get('movie_id')},
                {'$addToSet':
                    {
                        'movie_comment_info':  item['movie_comment_info']
                    }
                }, True)
        '''if isinstance(item,DoubandetailmoviereviewItem):
            self.db[item.collection].update(
                {'movie_id': item.get('movie_id')},
                {'$addToSet':
                    {
                        'movie_review_info': {'$each':item['movie_review_info']}
                    }
                }, True)'''

        if isinstance(item, DoubandetailmusicItem):
            self.db[item.collection].update({'music_id': item.get('music_id')}, {'$set': item}, True)
        if isinstance(item, DoubandetailmusiccommentItem) :
            self.db[item.collection].update(
                {'music_id': item.get('music_id')},
                {'$addToSet':
                    {
                        'music_comment_info':  item['music_comment_info']
                    }
                }, True)
        '''if isinstance(item,DoubandetailmusicreviewItem):
            self.db[item.collection].update(
                {'music_id': item.get('music_id')},
                {'$addToSet':
                    {
                    'music_review_info': {'$each':item['music_review_info']}
                    }
                }, True)'''
        if isinstance(item, DoubandetailbookItem):
            self.db[item.collection].update({'book_id': item.get('book_id')}, {'$set': item}, True)
        if isinstance(item, DoubandetailbookcommentItem) :
            self.db[item.collection].update(
                {'book_id': item.get('book_id')},
                {'$addToSet':
                    {
                        'book_comment_info':  item['book_comment_info']
                    }
                }, True)
        '''if isinstance(item,DoubandetailbookreviewItem):
            self.db[item.collection].update(
                {'book_id': item.get('book_id')},
                {'$addToSet':
                    {
                    'book_review_info': {'$each':item['book_review_info']}
                    }
                }, True)'''
        if isinstance(item, DoubanMovietIdItem):
            self.db[item.collection].update(
                 {'id': item.get('id')},
                {'$addToSet':
                    {
                        'movie_id': item['movie_id']

                    }
                }, True)

        if isinstance(item, DoubanMusictIdItem):
            self.db[item.collection].update(
                 {'id': item.get('id')},
                 {'$addToSet':
                    {
                        'music_id': item['music_id']
                     }
                 }, True)
        if isinstance(item, DoubanBookIdItem):
            self.db[item.collection].update(
                {'id': item.get('id')},
                {'$addToSet':
                    {
                        'book_id':  item['book_id']
                    }
                }, True)


        return item

