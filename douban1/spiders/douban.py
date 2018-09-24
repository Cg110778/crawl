# -*- coding: utf-8 -*-
import pymongo
import re

import scrapy
from scrapy import Request,Spider
from douban1.items import *

class DoubanSpider(Spider):
    name = 'douban1'
    allowed_domains = ['douban.com']
    start_urls = ['http://douban.com/people/{uid}/']
    contacts_url= 'https://www.douban.com/people/{uid}/contacts'
    rev_contacts_url = 'https://www.douban.com/people/{uid}/rev_contacts'
    user_url = 'http://douban.com/people/{uid}/'
    start_users = ['3452208']#'57109633',#'1693422','50446886'ninetonine''137546285'183501886'#'3452208'新桥，宋史
    movie_do_url = 'https://movie.douban.com/people/{uid}/do'
    movie_wish_url ='https://movie.douban.com/people/{uid}/wish'
    movie_collect_url ='https://movie.douban.com/people/{uid}/collect'
    music_do_url ='https://music.douban.com/people/{uid}/do'
    music_wish_url ='https://music.douban.com/people/{uid}/wish'
    music_collect_url ='https://music.douban.com/people/{uid}/collect'
    book_do_url ='https://book.douban.com/people/{uid}/do'
    book_wish_url ='https://book.douban.com/people/{uid}/wish'
    book_collect_url ='https://book.douban.com/people/{uid}/collect'
    contacts_id=[]
    movie_urls=[]
    music_urls=[]
    book_urls=[]

    def get_id(self,collection, database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        users_id= collection.find({},{'_id':0,'id':1})
        return users_id
        # print(users_id)
    def get_movie_urls(self,collection,database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        movie_url=collection.find({},{'_id':0,'movie_url':1})
        return movie_url

    def get_music_urls(self,collection,database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        music_url=collection.find({},{'_id':0,'music_url':1})
        return music_url

    def get_book_urls(self,collection,database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        book_url=collection.find({},{'_id':0,'book_url':1})
        return book_url

    '''def get_movie_comment_urls(self,collection,database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        movie_comment_url=collection.find({},{'_id':0,'movie_comment_url':1})
        return movie_comment_url

    def get_music_comment_urls(self, collection, database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        music_comment_url = collection.find({}, {'_id': 0, 'music_comment_url': 1})
        return music_comment_url

    def get_book_comment_urls(self, collection, database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        book_comment_url = collection.find({}, {'_id': 0, 'book_comment_url': 1})
        return book_comment_url

    def get_movie_review_urls(self,collection,database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        movie_review_url=collection.find({},{'_id':0,'movie_review_url':1})
        return movie_review_url

    def get_music_review_urls(self, collection, database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        music_review_url = collection.find({}, {'_id': 0, 'music_review_url': 1})
        return music_review_url

    def get_book_review_urls(self, collection, database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        book_review_url = collection.find({}, {'_id': 0, 'book_review_url': 1})
        return book_review_url'''






    def start_requests(self):
        for uid in self.start_users:
            yield Request(self.user_url.format(uid=uid), callback=self.parse_user)
    def parse_user(self,response):
        '''item=DoubanuserItem()
        item['id'] = response.xpath(
            '//*[@id="profile"]//div[@class="basic-info"]//div[@class="user-info"]//div[@class="pl"]/text()').re_first(
            '(\S+)')
        item['url']=response.url
        item['name'] = ''.join(
            response.xpath('//*[@id="db-usr-profile"]/div[@class="info"]/h1/text()').extract()).strip()
        item['img'] = response.xpath('//*[@id="profile"]//div[@class="basic-info"]/img/@src').extract_first()
        item['habitual_residence'] = response.xpath(
            '//*[@id="profile"]//div[@class="user-info"]/a/text()').extract_first()
        item['record_date'] = response.xpath(
            '//*[@id="profile"]//div[@class="basic-info"]//div[@class="user-info"]//div[@class="pl"]/text()').re_first(
            '(.*?)加入')
        item['user_display'] = ','.join(response.xpath(
            '//*[@id="profile"]//div[@class="user-intro"]//span[@id="intro_display"]/text()').extract()).strip()
        item['contacts_count'] = response.xpath('//*[@id="friend"]//span[@class="pl"]/a/text()').re_first('成员(\d+)')
        item['rev_contacts_count'] = response.xpath('//*[@class="rev-link"]/a/text()').re_first('.*?被(\d+)人关注')
        item['movie_do_number'] = response.xpath('//*[@id="movie"]/h2/span/a[1]/text()').extract_first()
        item['movie_wish_number'] = response.xpath('//*[@id="movie"]/h2/span/a[2]/text()').extract_first()
        item['movie_collect_number'] = response.xpath('//*[@id="movie"]/h2/span/a[3]/text()').extract_first()
        item['music_do_number'] = response.xpath('//*[@id="music"]/h2/span/a[1]/text()').extract_first()
        item['music_wish_number'] = response.xpath('//*[@id="music"]/h2/span/a[2]/text()').extract_first()
        item['music_collect_number'] = response.xpath('//*[@id="music"]/h2/span/a[3]/text()').extract_first()
        item['book_do_number'] = response.xpath('//*[@id="book"]/h2/span/a[1]/text()').extract_first()
        item['book_wish_number'] = response.xpath('//*[@id="book"]/h2/span/a[2]/text()').extract_first()
        item['book_collect_number'] = response.xpath('//*[@id="book"]/h2/span/a[3]/text()').extract_first()
        yield item'''
        #uid=response.xpath('//*[@id="profile"]//div[@class="basic-info"]//div[@class="user-info"]//div[@class="pl"]/text()').re_first('(\S+)')
        uids=self.get_id(collection='users',database='douban3')
        for uid in uids:
            if uid:
                uid=uid['id']
                #yield Request(self.contacts_url.format(uid=uid),callback=self.parse_contacts_list)
                #yield Request(self.rev_contacts_url.format(uid=uid), callback=self.parse_rev_contacts_list)
                yield Request(self.movie_do_url.format(uid=uid),callback=self.parse_movie_link)
                yield Request(self.movie_wish_url.format(uid=uid), callback=self.parse_movie_link)
                yield Request(self.movie_collect_url.format(uid=uid), callback=self.parse_movie_link)
                yield Request(self.music_do_url.format(uid=uid), callback=self.parse_music_link)
                yield Request(self.music_wish_url.format(uid=uid), callback=self.parse_music_link)
                yield Request(self.music_collect_url.format(uid=uid), callback=self.parse_music_link)
                yield Request(self.book_do_url.format(uid=uid), callback=self.parse_book_link)
                yield Request(self.book_wish_url.format(uid=uid), callback=self.parse_book_link)
                yield Request(self.book_collect_url.format(uid=uid), callback=self.parse_book_link)


    def parse_contacts_list(self, response):
        user_realation_item = UserRelationItem()
        result = re.search('https://www.douban.com/people/(.*?)/contacts', response.url)
        uid = result.group(1)
        contacts_list = response.xpath('//*[@id="content"]//div[@class="article"]//dl[@class="obu"]')
        for contact in contacts_list:
            contact_id = contact.xpath('./dd/a/@href').re_first('https://www.douban.com/people/(.*?)/')
            self.contacts_id.append(contact_id)
            name = contact.xpath('./dd/a/text()').extract_first()
            contacts = [{'id': contact_id, 'name': name}]
            user_realation_item['id'] = uid
            user_realation_item['contacts'] = contacts
            #user_realation_item['rev_contacts'] = []
            yield user_realation_item
            #print(len(self.contacts_id))
            if len(self.contacts_id)>241:
                return ''
            else:
                yield Request(self.user_url.format(uid=contact_id),callback=self.parse_user)


    def parse_rev_contacts_list(self, response):
        user_realation_item = UserRelationItem()
        result = re.search('https://www.douban.com/people/(.*?)/rev_contacts', response.url)
        uid = result.group(1)
        rev_contacts_list = response.xpath('//*[@id="content"]//div[@class="article"]//dl[@class="obu"]')
        for rev_contact in rev_contacts_list:
            rev_contact_id = rev_contact.xpath('./dd/a/@href').re_first('https://www.douban.com/people/(.*?)/')
            name = rev_contact.xpath('./dd/a/text()').extract_first()
            rev_contacts = [{'id': rev_contact_id, 'name': name}]
            user_realation_item['id'] = uid
            user_realation_item['contacts'] = []
            user_realation_item['rev_contacts'] = rev_contacts
            yield user_realation_item
            next_page = response.xpath(
                '//*[@id="content"]//div[@class="article"]//div[@class="paginator"]/span[@class="next"]/a/@href').extract_first()
            if next_page:
                next_page_url = 'https://www.douban.com' + next_page
                yield Request(url=next_page_url, callback=self.parse_rev_contacts_list)
                # 下一页关注用户的人
            yield Request(self.user_url.format(uid=rev_contact_id), callback=self.parse_user)

    def parse_movie_link(self,response):
        id = response.xpath('//*[@id="db-usr-profile"]/div[@class="info"]//li[1]/a/@href').extract_first()
        id = re.search('.*?/people/(.*?)/', id)
        id = id.group(1)
        movie_link=response.xpath(
            '//*[@id="content"]//div[@class="article"]//a[@class="nbg"]/@href').extract()
        #电影链接
        #item = response.meta['item']
        for i in movie_link:
            '''item=DoubanMovietIdItem()
            movie_id = re.search('https://movie.douban.com/subject/(.*?)/', i)
            movie_id = movie_id.group(1)
            item['movie_id'] = movie_id
            item['id'] = id
            yield item'''
            next_page = response.xpath(
                '//*[@id="content"]//div[@class="paginator"]/span[@class="next"]//a[contains(.,"后页")]/@href').extract_first()
            if next_page:
               if 'douban.com' in next_page:
                   yield Request(url=next_page, callback=self.parse_movie_link)
               else:
                    next_page_url = 'https://movie.douban.com' + next_page
                    # print(next_page,response.url)
                    yield Request(url=next_page_url, callback=self.parse_movie_link)
                    # 下一页subject列表'''
            movie_urls_mongodb=self.get_movie_urls(collection='movie',database='douban3')
            for movie_url in movie_urls_mongodb:
                if movie_url:
                    movie_url=movie_url['movie_url']
                    #print(movie_url)
                    if i in movie_url:#判断是否爬取过subject‘s url
                        return 'i in movie_urls'
                    else:
                        yield Request(url=i,callback=self.parse_movie)#电影简介'''
            yield Request(url=i,dont_filter=True, callback=self.parse_movie_detailink)#电影短评、影评


    def parse_music_link(self, response):
        id = response.xpath('//*[@id="db-usr-profile"]/div[@class="info"]//li[1]/a/@href').extract_first()
        id = re.search('.*?/people/(.*?)/', id)
        id = id.group(1)
        music_link = response.xpath('//*[@id="content"]//div[@class="article"]//a[@class="nbg"]/@href').extract()
        # 音乐链接
        for i in music_link:
            '''item = DoubanMusictIdItem()
            music_id = re.search('https://music.douban.com/subject/(.*?)/', i)
            music_id = music_id.group(1)
            item['music_id'] = music_id
            item['id'] = id
            yield item'''
            next_page = response.xpath(
                '//*[@id="content"]//div[@class="paginator"]/span[@class="next"]//a[contains(.,"后页")]/@href').extract_first()
            if next_page:
                if 'douban.com' in next_page:
                    yield Request(url=next_page, callback=self.parse_music_link)
                else:
                    next_page_url = 'https://music.douban.com' + next_page
                    yield Request(url=next_page_url, callback=self.parse_music_link)
                    # 下一页subject列表'''
            music_urls_mongodb = self.get_music_urls(collection='music', database='douban3')
            for music_url in music_urls_mongodb:
                if music_url:
                    music_url = music_url['music_url']
                    #print(music_url)
                    if i in music_url:#判断是否爬取过subject‘s url
                        return 'i in music_urls'
                    else:
                        yield Request(url=i, callback=self.parse_music)#音乐简介
            yield Request(url=i,dont_filter=True, callback=self.parse_music_detailink)#音乐短评、乐评'''


    def parse_book_link(self, response):
        id = response.xpath('//*[@id="db-usr-profile"]/div[@class="info"]//li[1]/a/@href').extract_first()
        id = re.search('.*?/people/(.*?)/', id)
        id = id.group(1)
        book_link = response.xpath('//*[@id="content"]//div[@class="article"]//a[@class="nbg"]/@href').extract()
        # 书籍链接
        for i in book_link:
            '''item = DoubanBookIdItem()
            book_id = re.search('https://book.douban.com/subject/(.*?)/', i)
            book_id = book_id.group(1)
            item['book_id'] = book_id
            item['id'] = id
            yield item'''
            next_page = response.xpath(
                '//*[@id="content"]//div[@class="paginator"]/span[@class="next"]//a[contains(.,"后页")]/@href').extract_first()
            if next_page:
                if 'douban.com' in next_page:
                    yield Request(url=next_page, callback=self.parse_book_link)
                else:
                    next_page_url = 'https://book.douban.com' + next_page
                    yield Request(url=next_page_url, callback=self.parse_book_link)
                    # 下一页subject列表'''
            book_urls_mongodb = self.get_book_urls(collection='book', database='douban3')
            for book_url in book_urls_mongodb:
                if book_url:
                    book_url = book_url['book_url']
                    #print(book_url)
                    if i in book_url:#判断是否爬取过subject‘s url
                        return 'i in book_urls'
                    else:
                        yield Request(url=i, callback=self.parse_book)#书籍简介
            yield Request(url=i,dont_filter=True,callback=self.parse_book_detailink)#书籍短评、书评'''



    def parse_movie(self,response):
        item = DoubandetailmovieItem()
        item['movie_url'] = response.url
        movie_id = re.search('https://movie.douban.com/subject/(.*?)/', response.url)
        movie_id = movie_id.group(1)
        item['movie_id'] = movie_id
        item['movie_name'] = response.xpath('//span[@property="v:itemreviewed"]/text()').extract_first()
        item['movie_playbill'] = response.xpath('//*[@id="mainpic"]/a/img/@src').extract_first()
        item['movie_director'] = response.xpath('//a[@rel="v:directedBy"]/text()').extract()
        item['movie_scriptwriter'] = response.xpath('//*[@id="info"]/span[2]/span[@class="attrs"]/a/text()').extract()
        item['movie_starring'] = response.xpath('//a[@rel="v:starring"]/text()').extract()
        item['movie_type'] = response.xpath('//span[@property="v:genre"]/text()').extract()
        item['movie_producer_countryORregion'] = response.selector.re(re.compile('<span.*?>制片国家/地区:</span>(.*?)<br>'))
        item['movie_language'] = response.selector.re(re.compile('<span.*?>语言:</span>(.*?)<br>'))
        item['movie_date'] = response.xpath('//span[@property="v:initialReleaseDate"]/text()').extract_first()
        item['movie_season'] = response.selector.re(re.compile('<span.*?>季数:</span>(.*?)<br>'))
        item['movie_episodes'] = response.selector.re(re.compile('<span.*?>集数:</span>(.*?)<br>'))
        item['movie_single_episode_length'] = response.selector.re(re.compile('<span.*?>单集片长:</span>(.*?)<br>'))
        item['movie_length'] = response.xpath('//span[@property="v:runtime"]/text()').extract_first()
        item['movie_alias'] = response.selector.re(re.compile('<span.*?>又名:</span>(.*?)<br>'))
        item['movie_IMDb'] = response.xpath('//*[@id="info"]/a/@href').extract_first()
        item['movie_star'] = response.xpath('//strong[@property="v:average"]/text()').extract_first()
        item['movie_5score'] = response.xpath(
            '//span[@class="stars5 starstop"]/../span[@class="rating_per"]/text()').extract_first()
        item['movie_4score'] = response.xpath(
            '//span[@class="stars4 starstop"]/../span[@class="rating_per"]/text()').extract_first()
        item['movie_3score'] = response.xpath(
            '//span[@class="stars3 starstop"]/../span[@class="rating_per"]/text()').extract_first()
        item['movie_2score'] = response.xpath(
            '//span[@class="stars2 starstop"]/../span[@class="rating_per"]/text()').extract_first()
        item['movie_1score'] = response.xpath(
            '//span[@class="stars1 starstop"]/../span[@class="rating_per"]/text()').extract_first()
        item['movie_describe'] = ''.join(response.xpath('//*[@id="link-report"]/span/text()').extract()).replace('\n',
                                                                                                                 '').strip()
        item['movie_comment_number'] = response.xpath(
            '//*[@id="comments-section"]//span[@class="pl"]/a/text()').extract_first()
        item['movie_review_number'] = response.xpath(
            '//*[@id="content"]//section[@class="reviews mod movie-content"]//span[@class="pl"]/a/text()').extract_first()
        yield item

    def parse_music(self,response):
        item = DoubandetailmusicItem()
        item['music_url'] = response.url
        music_id = re.search('https://music.douban.com/subject/(.*?)/', response.url)
        music_id = music_id.group(1)
        item['music_id'] = music_id
        item['music_alias'] = ''.join(response.selector.re(re.compile('<span.*?>又名:</span>(.*?).<br>', re.S))).strip()
        item['music_name'] = response.xpath('//*[@id="wrapper"]/h1/span/text()').extract_first()
        item['music__playbill'] = response.xpath('//*[@id="mainpic"]/span/a/img/@src').extract_first()
        item['music_performer'] = response.xpath(
            '//*[@id="content"]//*[@id="info"]//span[@class="pl"]/a/text()').extract()
        item['music_type'] = ''.join(response.selector.re(re.compile('<span.*?>流派:</span>(.*?)<br>', re.S))).strip()
        item['music_album_type'] = ''.join(
            response.selector.re(re.compile('<span.*?>专辑类型:</span>(.*?)<br>', re.S))).strip()
        item['music_medium'] = ''.join(response.selector.re(re.compile('<span.*?>介质:</span>(.*?)<br>', re.S))).strip()
        item['music_date'] = ''.join(response.selector.re(re.compile('<span.*?>发行时间:</span>(.*?)<br>', re.S))).strip()
        item['music_publisher'] = ''.join(
            response.selector.re(re.compile('<span.*?>出版者:</span>(.*?)<br>', re.S))).strip()
        item['music_number_of_records'] = ''.join(
            response.selector.re(re.compile('<span.*?>唱片数:</span>(.*?)<br>', re.S))).strip()
        item['music_barcode'] = ''.join(response.selector.re(re.compile('<span.*?>条形码:</span>(.*?)<br>', re.S))).strip()
        item['music_other_versions'] = ''.join(
            response.selector.re(re.compile('<span.*?>其他版本:</span>(.*?)<br>', re.S))).strip()
        item['music_star'] = response.xpath('//strong[@property="v:average"]/text()').extract_first()
        item['music_5score'] = response.xpath('//span[@class="rating_per"][1]/text()').extract_first()
        item['music_4score'] = response.xpath('//span[@class="rating_per"][2]/text()').extract_first()
        item['music_3score'] = response.xpath('//span[@class="rating_per"][3]/text()').extract_first()
        item['music_2score'] = response.xpath('//span[@class="rating_per"][4]/text()').extract_first()
        item['music_1score'] = response.xpath('//span[@class="rating_per"][5]/text()').extract_first()
        item['music_describe'] = ''.join(response.xpath('//*[@id="link-report"]/span/text()').extract()).replace('\n', '').strip()
        item['music_comment_number'] = response.xpath(
            '//*[@id="content"]//div[@class="mod-hd"]//span[@class="pl"]/a/text()').extract_first()
        item['music_review_number'] = response.xpath(
            '//*[@id="content"]//section[@class="reviews mod music-content"]//span[@class="pl"]/a/text()').extract_first()
        yield item

    def parse_book(self,response):
        item = DoubandetailbookItem()
        item['book_url'] = response.url
        book_id = re.search('https://book.douban.com/subject/(.*?)/', response.url)
        book_id = book_id.group(1)
        item['book_id'] = book_id
        item['book_url'] = response.url
        item['book_name'] = response.xpath('//*[@id="wrapper"]/h1/span/text()').extract_first()
        item['book_playbill'] = response.xpath('//*[@id="mainpic"]/a/img/@src').extract_first()
        item['book_star'] = response.xpath('//strong[@property="v:average"]/text()').extract_first()
        item['book_5score'] = response.xpath('//span[@class="rating_per"][1]/text()').extract_first()
        item['book_4score'] = response.xpath('//span[@class="rating_per"][2]/text()').extract_first()
        item['book_3score'] = response.xpath('//span[@class="rating_per"][3]/text()').extract_first()
        item['book_2score'] = response.xpath('//span[@class="rating_per"][4]/text()').extract_first()
        item['book_1score'] = response.xpath('//span[@class="rating_per"][5]/text()').extract_first()
        item['book_describe'] = ''.join(
            response.xpath('//div[@id="link-report"]//div[@class="intro"]//text()').extract()).replace(
            '\n', '').strip()
        item['book_comment_number'] = response.xpath(
            '//*[@id="content"]//div[@class="mod-hd"]//span[@class="pl"]/a/text()').extract_first()
        item['book_review_number'] = response.xpath(
            '//*[@id="content"]//section[@class="reviews mod book-content"]//span[@class="pl"]/a/text()').extract_first()
        datas = response.xpath("//div[@id='info']//text()").extract()
        datas = [data.strip() for data in datas]
        datas = [data for data in datas if data != ""]
        # 打印每一项内容
        # for i, data in enumerate(datas):
        # print "index %d " %i, data
        for data in datas:
            if u"作者" in data:
                if u":" in data:
                    item['book_author'] = ''.join(datas[datas.index(data) + 1]).strip()
                elif u":" not in data:
                    item['book_author'] = ''.join(datas[datas.index(data) + 2]).strip()
            elif u"出版社:" in data:
                item['book_publisher'] = datas[datas.index(data) + 1]
            elif u"译者:" in data:
                if u":" in data:
                    item['book_translator'] = datas[datas.index(data) + 1]
                elif u":" not in data:
                    item['book_translator'] = datas[datas.index(data) + 2]
            elif u"出版年:" in data:
                item['book_date'] = datas[datas.index(data) + 1]
            elif u"页数:" in data:
                item['book_page_number'] = datas[datas.index(data) + 1]
            elif u"定价:" in data:
                item['book_pricing'] = datas[datas.index(data) + 1]
            elif u"装帧:" in data:
                item['book_binding'] = datas[datas.index(data) + 1]
            elif u"丛书:" in data:
                if u":" in data:
                    item['book_series'] = datas[datas.index(data) + 1]
                elif u":" not in data:
                    item['book_series'] = datas[datas.index(data) + 2]
            elif u"ISBN:" in data:
                item['book_ISBN'] = datas[datas.index(data) + 1]
        yield item

    def parse_movie_detailink(self,response):
        movie_comment=response.xpath(
            '//*[@id="comments-section"]//div[@class="mod-hd"]//h2/span[@class="pl"]/a/@href').extract_first()
        # 电影短评的直接链接列表
        movie_review=response.url+'reviews'
        #影评的链接列表
        #yield Request(url=movie_comment, callback=self.parse_movie_comment)
        yield Request(url=movie_review,callback=self.parse_movie_review_list)


    def parse_music_detailink(self,response):
        music_comment=response.xpath('//div[@class="mod-hd"]//h2/span[@class="pl"]/a/@href').extract_first()
        # 音乐短评的链接列表
        music_review = response.url+'reviews'
        # 乐评的链接列表
        yield Request(url=music_comment, callback=self.parse_music_comment)
        yield Request(url=music_review, callback=self.parse_music_review_list)

    def parse_book_detailink(self,response):
        book_comment=response.xpath('//div[@class="mod-hd"]//h2/span[@class="pl"]/a/@href').extract_first()
        # 书籍短评的链接列表
        book_review = response.url+'reviews'
        # 书评的链接列表
        yield Request(url=book_comment, callback=self.parse_book_comment)
        #yield Request(url=book_review, callback=self.parse_book_review_list)

    '''def parse_movie_review_list(self,response):
        movie_review_list = response.xpath(
            '//*[@id="content"]//div[@class="review-list  "]/div//div[@class="main-bd"]/h2/a/@href').extract()
        for i in movie_review_list:
            movie_review_urls_mongodb = self.get_movie_review_urls(collection='movie', database='douban3')
            #print(movie_review_urls_mongodb)
            for movie_review_url in movie_review_urls_mongodb:
                if movie_review_url:
                    movie_review_url = movie_review_url['movie_review_url']
                    #print(movie_review_url)
                    if i in movie_review_url:  # 判断是否爬取过review‘s url
                        return 'i in movie_review_urls'
                    else:
                        yield Request(url=i,callback=self.parse_movie_review)
        movie_page = response.xpath(
            '//*[@id="content"]//div[@class="aside"]//div[@class="subject-title"]/a/@href').extract_first()
        next_page = response.xpath(
            '//*[@id="content"]//div[@class="paginator"]//span[@class="next"]/a/@href').extract_first()
        if next_page:
            if 'douban.com' in next_page:
                yield Request(url=next_page, callback=self.parse_movie_review_list)
            else:
                next_page_url = movie_page + 'reviews' + next_page
                #print(next_page, response.url)
                yield Request(url=next_page_url, callback=self.parse_movie_review_list)
                # 下一页影评列表'''

    '''def parse_music_review_list(self,response):
        music_review_list = response.xpath(
            '//*[@id="content"]//div[@class="review-list  "]/div//div[@class="main-bd"]/h2/a/@href').extract()
        for i in music_review_list:
            music_review_urls_mongodb = self.get_music_review_urls(collection='music', database='douban3')
            for music_review_url in music_review_urls_mongodb:
                if music_review_url:
                    music_review_url = music_review_url['music_review_url']
                    #print(music_review_url)
                    if i in music_review_url:  # 判断是否爬取过review‘s url
                        return 'i in music_review_urls'
                    else:
                        yield Request(url=i,callback=self.parse_music_review)
        music_page = response.xpath(
            '//*[@id="content"]//div[@class="aside"]//div[@class="subject-title"]/a/@href').extract_first()
        next_page = response.xpath(
            '//*[@id="content"]//div[@class="paginator"]//span[@class="next"]/a/@href').extract_first()
        if next_page:
            if 'douban.com' in next_page:
                yield Request(url=next_page, callback=self.parse_music_review_list)
            else:
                next_page_url = music_page + 'reviews' + next_page
                yield Request(url=next_page_url, callback=self.parse_music_review_list)
                # 下一页乐评列表'''


    '''def parse_book_review_list(self,response):
        book_review_list = response.xpath(
            '//*[@id="content"]//div[@class="review-list  "]/div//div[@class="main-bd"]/h2/a/@href').extract()
        for i in book_review_list:
            book_review_urls_mongodb = self.get_book_review_urls(collection='book', database='douban3')
            for book_review_url in book_review_urls_mongodb:
                if book_review_url:
                    book_review_url = book_review_url['book_review_url']
                    #print(book_review_url)
                    if i in book_review_url:  # 判断是否爬取过review‘s url
                        return 'i in book_review_urls'
                    else:
                        yield Request(url=i,callback=self.parse_book_review)
        book_page = response.xpath(
            '//*[@id="content"]//div[@class="aside"]//div[@class="subject-title"]/a/@href').extract_first()
        next_page = response.xpath(
            '//*[@id="content"]//div[@class="paginator"]//span[@class="next"]/a/@href').extract_first()
        if next_page:
            if 'douban.com' in next_page:
                yield Request(url=next_page, callback=self.parse_book_review_list)
            else:
                next_page_url = book_page + 'reviews' + next_page
                yield Request(url=next_page_url, callback=self.parse_book_review_list)
                # 下一页书评列表'''

    def parse_movie_comment(self,response):
        item = DoubandetailmoviecommentItem()
        movie_id = re.search('https://movie.douban.com/subject/(.*?)/comment.*?', response.url)
        movie_id = movie_id.group(1)
        #movie_comment_url = response.url
        for movie_comment in response.xpath('//div[@id="comments"]//div[@class="comment-item"]'):
            movie_commenter_name = movie_comment.xpath(
                './/span[@class="comment-info"]//a/text()').extract_first()
            movie_commenter_id = movie_comment.xpath(
                './/span[@class="comment-info"]/a').re_first('<a href="https://www.douban.com/people/(.*?)/"')
            movie_commenter_score = movie_comment.xpath(
                './/span[@class="comment-info"]/span').re_first('<span class="allstar(\d+)0.*?</span>')
            movie_comment_time = ''.join(movie_comment.xpath(
                './/span[@class="comment-info"]//span[@class="comment-time "]/text()').extract()).strip()
            movie_comment_useful_number = movie_comment.xpath(
                './/span[@class="comment-vote"]/span[@class="votes"]/text()').extract_first()
            movie_comment_content = ''.join(movie_comment.xpath(
                './/span[@class="short"]/text()').extract()).replace('\n', '').strip()
            movie_comment_info = [
                {'movie_commenter_name': movie_commenter_name, 'movie_commenter_id': movie_commenter_id,
                 'movie_commenter_score': movie_commenter_score, 'movie_comment_time': movie_comment_time,
                 'movie_comment_useful_number': movie_comment_useful_number,
                 'movie_comment_content': movie_comment_content}]
            #item['movie_comment_url'] = movie_comment_url
            item['movie_id'] = movie_id
            item['movie_comment_info'] = movie_comment_info
            yield item
            movie_page = response.xpath('//*[@id="content"]//div[@class="aside"]/p/a/@href').extract_first()
            next_page = response.xpath(
                '//*[@id="paginator"]/a[@class="next"]/@href').extract_first()
            if next_page:
                if 'douban.com' in next_page:
                    yield Request(url=next_page, callback=self.parse_movie_comment)
                else:
                    next_page_url = movie_page + 'comments' + next_page
                    #print(next_page, response.url)
                    yield Request(url=next_page_url, callback=self.parse_movie_comment)
                    # 下一页短评列表'''


    def parse_music_comment(self, response):
        item = DoubandetailmusiccommentItem()
        music_id = re.search('https://music.douban.com/subject/(.*?)/comment.*?', response.url)
        music_id = music_id.group(1)
        #music_comment_url=response.url
        for music_comment in response.xpath('//div[@id="comments"]//li[@class="comment-item"]'):
            music_commenter_name = music_comment.xpath('.//span[@class="comment-info"]//a/text()').extract_first()
            music_commenter_id = music_comment.xpath('.//span[@class="comment-info"]/a').re_first(
                '<a href="https://www.douban.com/people/(.*?)/"')
            music_commenter_score = music_comment.xpath('.//span[@class="comment-info"]/span').re_first(
                '<span class="user-stars allstar(\d+)0.*?</span>')
            music_comment_time = ''.join(music_comment.xpath(
                './/span[@class="comment-info"]//span/text()').extract()).strip()
            music_comment_useful_number = music_comment.xpath(
                './/span[@class="comment-vote"]/span/text()').extract_first()
            music_comment_content = ''.join(music_comment.xpath('.//span[@class="short"]/text()').extract()).replace(
                '\n', '').strip()
            music_comment_info = [
                {'music_commenter_name': music_commenter_name, 'music_commenter_id': music_commenter_id,
                 'music_commenter_score': music_commenter_score, 'music_comment_time': music_comment_time,
                 'music_comment_useful_number': music_comment_useful_number,
                 'music_comment_content': music_comment_content}]
            #item['music_comment_url'] = music_comment_url
            item['music_id'] = music_id
            item['music_comment_info'] = music_comment_info
            yield item
            music_page = response.xpath('//*[@id="content"]//div[@class="aside"]//p[2]/a/@href').extract_first()
            next_page = response.xpath(
                '//*[@id="paginator"]/a[@class="next"]/@href').extract_first()
            if next_page:
                if 'douban.com' in next_page:
                    yield Request(url=next_page, callback=self.parse_music_comment)
                else:
                    next_page_url = music_page + 'comments' + next_page
                    yield Request(url=next_page_url, callback=self.parse_music_comment)
                    # 下一页短评列表'''

    def parse_book_comment(self, response):
        item = DoubandetailbookcommentItem()
        book_id = re.search('https://book.douban.com/subject/(.*?)/comment.*?', response.url)
        book_id = book_id.group(1)
        #book_comment_url=response.url
        for book_comment in response.xpath('//div[@id="comments"]//li[@class="comment-item"]'):
            book_commenter_name = book_comment.xpath('.//span[@class="comment-info"]//a/text()').extract()
            book_commenter_id = book_comment.xpath('.//span[@class="comment-info"]/a').re_first(
                '<a href="https://www.douban.com/people/(.*?)/"')
            book_commenter_score = book_comment.xpath('.//span[@class="comment-info"]/span').re_first(
                '<span class="user-stars allstar(\d+)0.*?</span>')
            book_comment_time = ''.join(book_comment.xpath(
                './/span[@class="comment-info"]//span/text()').extract()).strip()
            book_comment_useful_number = book_comment.xpath(
                './/span[@class="comment-vote"]/span/text()').extract()
            book_comment_content = ''.join(
                book_comment.xpath('.//span[@class="short"]/text()').extract()).replace('\n', '').strip()
            book_comment_info = [
                {'book_commenter_name': book_commenter_name, 'book_commenter_id': book_commenter_id,
                 'book_commenter_score': book_commenter_score, 'book_comment_time': book_comment_time,
                 'book_comment_useful_number': book_comment_useful_number,
                 'book_comment_content': book_comment_content}]
            #item['book_comment_url']=book_comment_url
            item['book_id'] = book_id
            item['book_comment_info'] = book_comment_info
            yield item
            book_page = response.xpath('//*[@id="content"]//div[@class="aside"]//p[2]/a/@href').extract_first()
            next_page = response.xpath(
                '//*[@id="paginator"]/a[@class="next"]/@href').extract_first()
            if next_page:
                if 'douban.com' in next_page:
                    yield Request(url=next_page, callback=self.parse_book_comment)
                else:
                    next_page_url = book_page + 'comments' + next_page
                    yield Request(url=next_page_url, callback=self.parse_book_comment)
                    # 下一页短评列表'''

    '''def parse_movie_review(self,response):
        item = DoubandetailmoviereviewItem()
        movie_review_url=response.url
        movie_id = response.xpath(
            '//*[@id="content"]//div[@class="subject-title"]/a/@href').extract_first()
        r_movie_id = re.search('.*?https://movie.douban.com/subject/(.*?)/.*?', movie_id)
        f_movie_id = r_movie_id.group(1)
        movie_reviewer_name = response.xpath(
            '//*[@id="content"]//div[@class="article"]//header[@class="main-hd"]//span[@property="v:reviewer"]/text()').extract_first()
        movie_reviewer_id = response.xpath(
            '//*[@id="content"]//div[@class="article"]//header[@class="main-hd"]/a').re_first(
            '<a href="https://www.douban.com/people/(.*?)/"')
        movie_reviewer_score = response.xpath(
            '//*[@id="content"]//div[@class="article"]//header[@class="main-hd"]/span').re_first(
            '<span class="allstar(\d+)0.*?</span>')
        movie_review_time = ''.join(response.xpath(
            '//*[@id="content"]//div[@class="article"]//header[@class="main-hd"]//span[@property="v:dtreviewed"]/text()').extract()).strip()
        movie_review_title = response.xpath(
            '//*[@id="content"]//div[@class="article"]//span[@property="v:summary"]/text()').extract_first()
        movie_review_content = ''.join(response.xpath(
            '//*[@id="content"]//div[@class="article"]//div[@class="main-bd"]//div[@property="v:description"]//text()').extract()).replace(
            '\n', '').strip()
        movie_review_useful_number = response.xpath(
            '//*[@id="content"]//div[@class="article"]//div[@class="main-ft"]//button/text()').re_first(
            '.*?有用 (\d+).*?')
        movie_review_useless_number = response.xpath(
            '//*[@id="content"]//div[@class="article"]//div[@class="main-ft"]//button/text()').re_first(
            '.*?没用 (\d+).*?')
        movie_review_info = [{'movie_reviewer_name': movie_reviewer_name, 'movie_reviewer_id': movie_reviewer_id,
                              'movie_reviewer_score': movie_reviewer_score, 'movie_review_time': movie_review_time,
                              'movie_review_title': movie_review_title, 'movie_review_content': movie_review_content,
                              'movie_review_useful_number': movie_review_useful_number,
                              'movie_review_useless_number': movie_review_useless_number}]
        item['movie_review_url']=movie_review_url
        item['movie_id'] = f_movie_id
        item['movie_review_info'] = movie_review_info
        yield item



    def parse_music_review(self,response):
        item = DoubandetailmusicreviewItem()
        music_review_url = response.url
        music_id = response.xpath(
            '//*[@id="content"]//div[@class="subject-title"]/a/@href').extract_first()
        r_music_id = re.search('.*?https://music.douban.com/subject/(.*?)/.*?', music_id)
        f_music_id = r_music_id.group(1)
        music_reviewer_name = response.xpath(
            '//*[@id="content"]//div[@class="article"]//header[@class="main-hd"]//span[@property="v:reviewer"]/text()').extract_first()
        music_reviewer_id = response.xpath(
            '//*[@id="content"]//div[@class="article"]//header[@class="main-hd"]/a').re_first(
            '<a href="https://www.douban.com/people/(.*?)/"')
        music_reviewer_score = response.xpath(
            '//*[@id="content"]//div[@class="article"]//header[@class="main-hd"]/span').re_first(
            '<span class="allstar(\d+)0.*?</span>')
        music_review_time = ''.join(response.xpath(
            '//*[@id="content"]//div[@class="article"]//header[@class="main-hd"]//span[@property="v:dtreviewed"]/text()').extract()).strip()
        music_review_title = response.xpath(
            '//*[@id="content"]//div[@class="article"]//span[@property="v:summary"]/text()').extract_first()
        music_review_content = ''.join(response.xpath(
            '//*[@id="content"]//div[@class="article"]//div[@class="main-bd"]//div[@property="v:description"]//text()').extract()).replace(
            '\n', '').strip()
        music_review_useful_number = response.xpath(
            '//*[@id="content"]//div[@class="article"]//div[@class="main-ft"]//button/text()').re_first(
            '.*?有用 (\d+).*?')
        music_review_useless_number = response.xpath(
            '//*[@id="content"]//div[@class="article"]//div[@class="main-ft"]//button/text()').re_first(
            '.*?没用 (\d+).*?')
        music_review_info = [{'music_reviewer_name': music_reviewer_name, 'music_reviewer_id': music_reviewer_id,
                              'music_reviewer_score': music_reviewer_score, 'music_review_time': music_review_time,
                              'music_review_title': music_review_title,
                              'music_review_content': music_review_content,
                              'music_review_useful_number': music_review_useful_number,
                              'music_review_useless_number': music_review_useless_number}]
        item['music_review_url'] = music_review_url
        item['music_id'] = f_music_id
        item['music_review_info'] = music_review_info
        yield item


    def parse_book_review(self,response):
        item = DoubandetailbookreviewItem()
        book_review_url = response.url
        book_id = response.xpath(
            '//*[@id="content"]//div[@class="subject-title"]/a/@href').extract_first()
        r_book_id = re.search('.*?https://book.douban.com/subject/(.*?)/.*?', book_id)
        f_book_id = r_book_id.group(1)
        book_reviewer_name = response.xpath(
            '//*[@id="content"]//div[@class="article"]//header[@class="main-hd"]//span[@property="v:reviewer"]/text()').extract_first()
        book_reviewer_id = response.xpath(
            '//*[@id="content"]//div[@class="article"]//header[@class="main-hd"]/a').re_first(
            '<a href="https://www.douban.com/people/(.*?)/"')
        book_reviewer_score = response.xpath(
            '//*[@id="content"]//div[@class="article"]//header[@class="main-hd"]/span').re_first(
            '<span class="allstar(\d+)0.*?</span>')
        book_review_time = ''.join(response.xpath(
            '//*[@id="content"]//div[@class="article"]//header[@class="main-hd"]//span[@property="v:dtreviewed"]/text()').extract()).strip()
        book_review_title = response.xpath(
            '//*[@id="content"]//div[@class="article"]//span[@property="v:summary"]/text()').extract_first()
        book_review_content = ''.join(response.xpath(
            '//*[@id="content"]//div[@class="article"]//div[@class="main-bd"]//div[@property="v:description"]//text()').extract()).replace(
            '\n', '').strip()
        book_review_useful_number = response.xpath(
            '//*[@id="content"]//div[@class="article"]//div[@class="main-ft"]//button/text()').re_first(
            '.*?有用 (\d+).*?')
        book_review_useless_number = response.xpath(
            '//*[@id="content"]//div[@class="article"]//div[@class="main-ft"]//button/text()').re_first(
            '.*?没用 (\d+).*?')
        book_review_info = [{'book_reviewer_name': book_reviewer_name, 'book_reviewer_id': book_reviewer_id,
                             'book_reviewer_score': book_reviewer_score, 'book_review_time': book_review_time,
                             'book_review_title': book_review_title,
                             'book_review_content': book_review_content,
                             'book_review_useful_number': book_review_useful_number,
                             'book_review_useless_number': book_review_useless_number}]
        item['book_review_url'] = book_review_url
        item['book_id'] = f_book_id
        item['book_review_info'] = book_review_info
        yield item'''


