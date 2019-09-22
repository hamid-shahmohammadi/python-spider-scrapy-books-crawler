# -*- coding: utf-8 -*-
import os
import csv
import glob
import MySQLdb
from scrapy import Spider
from scrapy.http import Request


def product_inf(response,value):
    return response.xpath('//th[text()="' + value + '"]/following-sibling::td/text()').extract_first()


class BooksSpider(Spider):
    name = 'books'
    allowed_domains = ['books.toscrape.com']
    start_urls=['http://books.toscrape.com']


    def parse(self,response):
        books = response.xpath('//h3/a/@href').extract()
        for book in books:
            absolute_url=response.urljoin(book)
            yield Request(absolute_url,callback=self.parse_book)

        # next_page_url = response.xpath('//a[text()="next"]/@href').extract_first()
        # absolute_next_page_url = response.urljoin(next_page_url)
        # yield Request(absolute_next_page_url)


    def parse_book(self,response):
        title = response.css('h1::text').extract_first()
        rating = response.xpath('//*[contains(@class,"star-rating")]/@class').extract_first()
        rating = rating.replace('star-rating ', '')
        UPC = product_inf(response, 'UPC')
        product_type = product_inf(response, 'Product Type')

        yield {
            'rating': rating,
            'product_type': product_type,
            'UPC': UPC,
            'title': title,
        }

    def close(self, reason):
        csv_file = max(glob.iglob('*.csv'), key=os.path.getctime)
        mydb=MySQLdb.connect(host='localhost',user='root',passwd='123456',db='books_db')

        cursor=mydb.cursor()

        csv_data=csv.reader(file(csv_file))

        row_count = 0
        for row in csv_data:
            print row
            #if row_count != 0:
            cursor.execute('INSERT IGNORE INTO books_table(rating, product_type, upc, title) VALUES(%s, %s, %s, %s)', row)
            row_count += 1

        mydb.commit()
        cursor.close()

