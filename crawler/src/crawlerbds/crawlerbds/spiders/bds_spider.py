import scrapy
from urllib.parse import urljoin
from crawlerbds.items import BatdongsanItem


class BdsSpiderSpider(scrapy.Spider):
    name = "bds_spider"
    allowed_domains = ["bds.com.vn"]

    def __init__(self, min_page=-1, max_page=999999, province='ha-noi', jump_to_page=-1, estate_type=0):
        super().__init__()
        self.min_page = int(min_page)
        self.max_page = int(max_page)
        self.province = province
        self.jump_to_page = int(jump_to_page)
        self.estate_type = int(estate_type)

    def _get_page_url(self, province, page_num):
        arg_map = {'province': province, 'page_num': page_num}
        first_page_link = 'https://bds.com.vn/mua-ban-nha-dat-{province}'
        page_link = 'https://bds.com.vn/mua-ban-nha-dat-{province}-page{page_num}'
        if page_num == 1:
            return first_page_link.format_map(arg_map)
        else:
            return page_link.format_map(arg_map)  

    def start_requests(self):
        domain_url = 'https://bds.com.vn/'
        if self.estate_type == 0:
            start_url = domain_url + "mua-ban-nha-mat-pho"
        elif self.estate_type == 1:
            start_url = domain_url + "mua-ban-nha-rieng"
        elif self.estate_type == 2:
            start_url = domain_url + "mua-ban-can-ho-chung-cu"
        else:
            start_url = domain_url + "mua-ban-nha-biet-thu-lien-ke"
        yield scrapy.Request(url=start_url, callback=self.parse_home_page)

    def parse_home_page(self, response):

        province_page_links = response.css('ul.list-menu-nhadat-service li h3 a::attr(href)').getall()

        for province_page_link in province_page_links:
            if self.estate_type == 0:
                province = province_page_link.split('/')[-1].replace('mua-ban-nha-mat-pho-', '')
            elif self.estate_type == 1:
                province = province_page_link.split('/')[-1].replace('mua-ban-nha-rieng-', '')
            elif self.estate_type == 2:
                province = province_page_link.split('/')[-1].replace('mua-ban-can-ho-chung-cu-', '')
            else:
                province = province_page_link.split('/')[-1].replace('mua-ban-nha-biet-thu-lien-ke-', '')
            if province == self.province:
                province_full_page_link = province_page_link
                if self.min_page > 1:
                    province_full_page_link += ("-page" + str(self.min_page))
                yield scrapy.Request(url=province_full_page_link, callback=self.parse_province_page, meta={'province': province, 'page': self.min_page})


    def parse_province_page(self, response):
        province = response.meta['province']
        page = response.meta['page']

        if self.jump_to_page > 1:
            dest_page_num = self.jump_to_page
            self.jump_to_page = -1
            yield scrapy.Request(url=self._get_page_url(province=province, page_num=dest_page_num), callback=self.parse_province_page, meta={'province': province, 'page': dest_page_num})
        else:
            if page >= self.min_page:
                real_estates = response.css('div.item-nhadat')
                for real_estate in real_estates:
                    real_estate_link = real_estate.css('a.title-item-nhadat::attr(href)').get()
                    yield scrapy.Request(url=real_estate_link, callback=self.parse_real_estate_page, meta={'province': province})

            if page == 1:
                next_page = response.css('div.pagination.row-cl a.other-page::attr(href)')[0].get()
            else:
                next_page = response.css('div.pagination.row-cl a.next-page::attr(href)').get()
            if next_page is not None and page < self.max_page:
                next_page = urljoin('https://bds.com.vn/', next_page)
                yield scrapy.Request(url=next_page, callback=self.parse_province_page, meta={'province': province, 'page': page+1})
    

    def parse_real_estate_page(self, response):
        bds_item = BatdongsanItem()

        bds_item['title'] = response.css('h1.title-product::text').get()
        bds_item['description'] = '\n'.join([line.strip() for line in response.css('div.ct-pr-sum::text').getall()])
        bds_item['price'] = response.css('ul.list-attr-hot')[0].css('li')[0].css('span.value-attr::text').get()
        bds_item['square'] = response.css('ul.list-attr-hot')[0].css('li')[1].css('span.value-attr::text').get()
        bds_item['address'] = {'full_address': None, 'province': None, 'district': None, 'ward': None}
        bds_item['address']['province'] = response.meta['province']
        if response.css('.breadcrumb li').get() is not None:
            bds_item['address']['full_address'] = ', '.join([add_info.strip() for add_info in response.css('div.product_base ul.breadcrumb li a::text').getall()[1:][::-1]])
        else:
            bds_item['address']['full_address'] = None
        if self.estate_type == 0:
            bds_item["estate_type"] = "Nhà phố"
        elif self.estate_type == 1:
            bds_item["estate_type"] = "Nhà riêng"
        elif self.estate_type == 2:
            bds_item["estate_type"] = "Chung cư"
        else:
            bds_item["estate_type"] = "Biệt thự"
        bds_item['post_date'] = response.css('ul.list-attr-hot')[0].css('li')[2].css('span.value-attr2::text').get()
        bds_item['post_id'] = response.css('ul.list-attr-hot')[0].css('li')[3].css('span.value-attr2::text').get()

        contact_info = {}
        contact_info['name'] = response.css('div.content-info-member span.member-name::text').get()
        phones = response.css('div.row-contact-product a.hotline-member-detail ::attr(href)').getall()
        contact_info["phone"] = [phone.replace("tel:","") for phone in phones]
        bds_item['contact_info'] = contact_info

        extra_infos = {}
        if response.css('ul.list-attr-hot')[1].css('li'):
            for extra_info in response.css('ul.list-attr-hot')[1].css('li'):
                label = extra_info.css('span::text').getall()[0]
                value = extra_info.css('span::text').getall()[1]
                extra_infos[label] = value

        bds_item['extra_infos'] = extra_infos

        bds_item['link'] = response.url
        yield bds_item


