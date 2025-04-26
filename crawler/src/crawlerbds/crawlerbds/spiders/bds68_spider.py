import time
from typing import Iterable
import scrapy
from urllib.parse import urljoin
from scrapy.http import Request
from datetime import date, timedelta
from crawlerbds.items import BatdongsanItem
import re


class IBatdongsanSpider(scrapy.Spider):
    name = "bds68_spider"
    allowed_domains = ["bds68.com.vn"]

    def __init__(self, min_page=-1, max_page=999999, province='ha-noi', jump_to_page=-1, estate_type=0):
        super().__init__()
        self.min_page = int(min_page)
        self.max_page = int(max_page)
        self.province = province
        self.jump_to_page = int(jump_to_page)
        self.estate_type = int(estate_type)

    def _get_page_url(self, province, page_num):
        arg_map = {'province': province, 'page_num': page_num}
        first_page_link = 'https://i-batdongsan.com/can-ban-nha-dat/{province}-t1.htm'
        page_link = 'https://i-batdongsan.com/can-ban-nha-dat/{province}-t1/p{page_num}.htm'
        if page_num == 1:
            return first_page_link.format_map(arg_map)
        else:
            return page_link.format_map(arg_map)

    def start_requests(self):
        domain_url = 'https://bds68.com.vn/'
        if self.estate_type == 0:
            start_url = domain_url + "ban-nha-mat-tien/"
        elif self.estate_type == 1:
            start_url = domain_url + "ban-nha-rieng/"
        elif self.estate_type == 2:
            start_url = domain_url + "ban-chung-cu/"
        else:
            start_url = domain_url + "ban-nha-biet-thu-du-an/"
        yield scrapy.Request(url=start_url, callback=self.parse_home_page)

    def parse_home_page(self, response):

        # Discover real estate page for each province
        province_page_links = response.css('div.readmore-box ul li a::attr(href)').getall()

        for province_page_link in province_page_links:
            province = province_page_link.split('/')[-1]
            if province == self.province:
                province_full_page_link = urljoin('https://bds68.com.vn/', province_page_link) + "?sx=1" if self.min_page==1 else urljoin('https://bds68.com.vn/', province_page_link) + "?sx=1&pg=" + str(self.min_page)
                yield scrapy.Request(url=province_full_page_link, callback=self.parse_province_page,
                                     meta={'province': province, 'page': self.min_page})

    def parse_province_page(self, response):
        province = response.meta['province']
        page = response.meta['page']

        if self.jump_to_page > 1:
            dest_page_num = self.jump_to_page
            self.jump_to_page = -1
            yield scrapy.Request(url=self._get_page_url(province=province, page_num=dest_page_num),
                                 callback=self.parse_province_page, meta={'province': province, 'page': dest_page_num})
        else:
            if page >= self.min_page:
                # Discover page links for province 's real estate
                real_estates = response.css('div.prop-box-item-contain div div.div_prop_wapper.clearfix')
                for real_estate in real_estates:
                    real_estate_link = real_estate.css('div.div_prop_info div h4 a::attr(href)').get()
                    real_estate_link = urljoin('https://bds68.com.vn/', real_estate_link)
                    yield scrapy.Request(url=real_estate_link, callback=self.parse_real_estate_page,
                                         meta={'province': province})

            # Go to next page
            page_links = response.css('div.paging div div div ul li a::attr(href)').getall()[1:-1]
            page_numbers = [page_link.split("pg=")[-1] for page_link in page_links]
            current_page = response.css('div.paging div div div ul li.active span::text').get()
            if int(current_page) == 1:
                current_page_idx = 0
            else:
                current_page_idx = int(current_page) - int(page_numbers[0])
            if current_page_idx >= len(page_links):
                next_page = None
            else:
                next_page = page_links[current_page_idx]
            if next_page is not None and page < self.max_page:
                next_page = urljoin('https://bds68.com.vn/', next_page)
                yield scrapy.Request(url=next_page, callback=self.parse_province_page,
                                     meta={'province': province, 'page': page + 1})

    def parse_real_estate_page(self, response):
        time.sleep(5)
        bds_item = BatdongsanItem()

        bds_item['title'] = response.css('h1.detail-prop-title ::text').get()
        bds_item['description'] = response.css('div.prop-description div p::text').get()
        bds_item['address'] = {'full_address': None, 'province': None, 'district': None, 'ward': None}
        bds_item['address']['province'] = response.meta['province']
        bds_item['address']['full_address'] = response.css('div.prop-address a span::text').get()

        contact_info = {}
        contact_info['name'] = response.css('div.seller-info-container a div.seller-info-header div h3::text').get()
        contact_info['phone'] = [phone for phone in
                                 response.css('div.seller-info-container a div.seller-info-phone div span.show_phone ::text').getall()]
        bds_item['contact_info'] = contact_info

        extra_infos = {}
        info_rows = response.css('div.prop-features div::text').getall()
        for row in range(3 ,len(info_rows)):
            row_info = info_rows[row]
            parts = row_info.split(":")
            if(len(parts) != 2):
                continue
            if "Nội thất" in parts[0] or "nội thất" in parts[0]:
                continue
            if self.estate_type == 0:
                bds_item["estate_type"] = "Nhà phố"
            elif self.estate_type == 1:
                bds_item["estate_type"] = "Nhà riêng"
            elif self.estate_type == 2:
                bds_item["estate_type"] = "Chung cư"
            else:
                bds_item["estate_type"] = "Biệt thự"

            if re.sub(r'\s+', ' ', parts[0]).strip() == "Giá":
                bds_item['price'] = re.sub(r'\s+', ' ', parts[1]).strip()
            elif re.sub(r'\s+', ' ', parts[0]).strip() == "Diện Tích":
                bds_item['square'] = re.sub(r'\s+', ' ', parts[1]).strip()
            elif re.sub(r'\s+', ' ', parts[0]).strip() == "Mã Đăng Tin":
                bds_item["post_id"] = re.sub(r'\s+', ' ', parts[1]).strip()
            elif re.sub(r'\s+', ' ', parts[0]).strip() == "Ngày Đăng":
                bds_item["post_date"] = re.sub(r'\s+', ' ', parts[1]).split("-")[0].strip()
            else:
                extra_infos[re.sub(r'\s+', ' ', parts[0]).strip()] = re.sub(r'\s+', ' ', parts[1]).strip()

        bds_item['extra_infos'] = extra_infos

        bds_item['link'] = response.url
        yield bds_item


