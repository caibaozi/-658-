# -*- coding: utf-8 -*-
import re
import scrapy
from soufangwang.items import NewHouseItem,ESFHouseItem
from scrapy_redis.spiders import RedisSpider
class SfwSpider(RedisSpider):
    name = 'sfw'
    allowed_domains = ['fang.com']
    # start_urls = ['https://www.fang.com/SoufunFamily.htm']
    redis_key = 'soufangwang:start_url'
    def parse(self, response):
        trs = response.xpath('//div[@class="outCont"]//tr')
        province = None
        for tr in trs:
            tds = tr.xpath(".//td[not(@class)]")
            province_td = tds[0]
            province_text = province_td.xpath('.//text()').get()
            province_text = re.sub(r"\s","",province_text)
            if province_text:
                province = province_text
            if province =='其它':
                continue
            city_td = tds[1]
            city_links = city_td.xpath('.//a')
            for city_link in city_links:
                city = city_link.xpath(".//text()").get()
                city_url = city_link.xpath(".//@href").get()
                url_modeule = city_url.split('//')
                scheme = url_modeule[0]
                domain = url_modeule[1]
                if 'bj.' in domain:
                    newhouse_url= 'http://newhouse.fang.com/house/s/'
                    esf_url = 'http://esf.fang.com/'
                else:
                    newhouse_url = scheme + '//' + 'newhouse.' + domain + 'howse/s/'
                    esf_url = scheme + '//' + 'esf.' + domain
                yield scrapy.Request(url = newhouse_url,callback= self.parse_newhouse,meta={'info':(province,city)})
                yield scrapy.Request(url = esf_url,callback=self.parse_esf,meta={'info':(province,city)})
    def parse_newhouse(self,response):
        province,city = response.meta.get('info')
        lis = response.xpath('//div[contains(@class,"nl_con")]/ul/li')
        for li in lis:
            name = li.xpath('//div[@class="nlcd_name"]/a/text()').get().strip()
            huose_type_list = li.xpath('.//div[contains(@class,"house_type")]/a/text()').getall()
            huose_type_list =list(map(lambda x:re.sub(r"\s",'',x),huose_type_list))
            rooms = list(filter(lambda x:x.endswith("居"), huose_type_list))
            area = ''.join(li.xpath('.//div[contains(@class,"house_type")]/text()').getall())
            area = re.sub(r'\s|－ |/','',area)
            address = li.xpath(".//div[@class='address']/a/@title").get()
            district_text = ''.join(li.xpath(".//div[@class='address']/a//text()").getall())
            district = re.search(r'.*\[(.+)\].*',district_text)
            if district:
                   district=district.group(1)
            else:
                district = '空'
            sale = li.xpath(".//div[contains(@class,'fangyuan')]/span/text()").get()
            price = ''.join(li.xpath('.//div[@class="nhouse_price"]//text()').getall())
            price = re.sub(r"\s|广告","",price)
            origin_url = li.xpath(".//div[@class='nlcd_name']/a/@href").get()
            item = NewHouseItem(name=name,rooms=rooms,area=area,address=address,district=district,sale=sale,price=price,origin_url=origin_url,province=province,city=city)
            yield item
        next_url = response.xpath("//div[@class='page']//a[@class='next']/@href").get()
        if next_url:
            yield scrapy.Request(url=response.urljonin(next_url), callback=self.parse_newhouse, meta={'info': (province, city)})
    def parse_esf(self,response):
        province,city = response.meta.get('info')
        dls = response.xpath("//div[@class='shop_list shop_list_4']/dl")
        for dl in dls:
            item = ESFHouseItem(province=province,city=city)
            item['name'] = dl.xpath('.//p[@class="add_shop"]/a/@title').get()
            infos = dl.xpath('.//p[@class="tel_shop"]/text()').getall()
            infos = list(map(lambda  x:re.sub(r"\s","",x),infos))
            for info in infos:
                if "厅" in info:
                    item['rooms'] = info
                elif '层' in info:
                    item['floor'] = info
                elif '向' in info:
                    item['toward'] = info
                elif '㎡' in  info:
                    item['area'] = info
                else:
                    item['year'] = info.replace('建筑年代:','')
            address = dl.xpath('.//p[@class="add_shop"]/span/text()').get()
            item['address'] = address
            unit = dl.xpath('.//dd[@class="price_right"]/span[2]/text()').get()
            item['unit'] = unit
            price = dl.xpath('.//dd[@class="price_right"]/span/b/text()').get()
            if price:
                item['price'] = str(price)+'万'
            else:
                item['price'] = price
            origin_url = dl.xpath('.//h4[@class="clearfix"]/a/@href').get()
            item['origin_url'] = response.urljoin(origin_url)

            yield item
        next_url = response.xpath('//div[@class="page_al"]/p/a/@href').get()
        yield scrapy.Request(url=response.urljoin(next_url),callback=self.parse_esf,meta={'info':(province,city)})