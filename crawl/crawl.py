from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
from typing import Callable, Dict, Optional
import requests
import re
import json
import os
import shutil
from pathlib import Path


class URL:
    def __init__(self, homepage="https://thuvienphapluat.vn/page/tim-van-ban.aspx?keyword=&area=0&type=0&status=0&lan=1&org=15&signer=0&match=True&sort=2&bdate=01/01/2000&edate=26/05/2025"):
        self.homepage = homepage
        options = Options()
        options.add_argument('--headless')                         # chạy ẩn
        options.add_argument('--disable-gpu')
        options.add_argument('--log-level=3')                      # chỉ hiện lỗi nghiêm trọng
        options.add_argument('--window-size=1920,1080')            # ổn định layout khi headless
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        driver = None
        try:
            driver_path = ChromeDriverManager().install()
            service = Service(driver_path)
            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            # không thể tải driver tự động
            print("Warning: webdriver-manager failed to download ChromeDriver:", e)
            # Kiểm tra biến môi trường CHROME_DRIVER_PATH
            env_path = os.environ.get('CHROME_DRIVER_PATH')
            if env_path and Path(env_path).exists():
                print(f"Using CHROME_DRIVER_PATH at {env_path}")
                service = Service(env_path)
                driver = webdriver.Chrome(service=service, options=options)
            else:
                # Tìm chromedriver trên PATH
                which = shutil.which('chromedriver') or shutil.which('chromedriver.exe')
                if which:
                    print(f"Found chromedriver on PATH: {which}")
                    service = Service(which)
                    driver = webdriver.Chrome(service=service, options=options)
                else:
                    raise RuntimeError(
                        "Could not obtain ChromeDriver. Either enable internet access so webdriver-manager can download it, "
                        "or download chromedriver manually and set CHROME_DRIVER_PATH to its full path, or add it to PATH."
                    )

        # At this point driver should be set or exception raised
        self.driver = driver
        self.data = []
        self.docs = []
        self.parser = Parser()

    def crawl1page(self, url) -> int:
        """
        href i (1<=i <=20) nằm trong card:
        #block-info-advan > div:nth-child(2) > div:nth-child(i) > div.left-col > div.nq > p.nqTitle > a
        """
        try:
            self.driver.get(url)
            wait = WebDriverWait(self.driver, 10)  # tăng timeout cho ổn định hơn
            data = []
            for i in range(1, 21):
                try:
                    element = wait.until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR,
                             f"#block-info-advan > div:nth-child(2) > div:nth-child({i}) > div.left-col > div.nq > p.nqTitle > a")
                        )
                    )
                    link = element.get_attribute('href')
                    text = element.text or ""
                    data.append({'name': text, 'link': link})
                except TimeoutException:
                    print(f"Finish crawl page (early stop), {len(data)} documents found on this page!")
                    self.data.extend(data)
                    return 400
            self.data.extend(data)
            return 200
        except WebDriverException:
            return 404

    def crawl_links(self, limit=100000):
        try:
            status = 200
            numPage = 1
            while status == 200 and len(self.data) + 20 < limit:
                page = self.homepage + f'&page={numPage}'
                print(f"crawl page {numPage} -> {page}")
                numPage += 1
                status = self.crawl1page(page)
            print(f"Finish crawling! Number of documents: {len(self.data)}")
        except WebDriverException as e:
            print("WebDriverException while crawling links:", e)

    def crawl_content(self, link):
        try:
            self.driver.get(link)
            wait = WebDriverWait(self.driver, 10)  # tăng timeout
            element = wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "#divContentDoc > div.content1")
                )
            )
            html = element.get_attribute('outerHTML') or ""
            text = element.text or ""
            return text, html
        except (WebDriverException, TimeoutException):
            return None, None

    def crawl_docs(self, start=0, end=100000):
        if start >= len(self.data):
            return None
        end = min(end, len(self.data))
        sub_data = self.data[start:end]
        for i, element in enumerate(sub_data, start=0):
            name = element.get('name', '')
            link = element.get('link', '')
            text, doc_src = self.crawl_content(link)
            if text is None and doc_src is None:
                print(f"[WARN] Skip (failed to fetch) -> {link}")
                continue
            doc = {'name': name, 'content': text or '', 'src': doc_src or ''}
            self.docs.append(doc)
            if (i + 1) % 20 == 0:
                print(f"Fetched {i + 1} documents in range [{start}, {end})")

    def save_links(self, path, file_name):
        os.makedirs(path, exist_ok=True)
        file_path = os.path.join(path, file_name)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=4, ensure_ascii=False)
        print(f"Finish saving links to: {file_path}")

    def save_docs(self, path_html, path_content, start=0):
        os.makedirs(path_html, exist_ok=True)
        os.makedirs(path_content, exist_ok=True)
        for i, element in enumerate(self.docs):
            content = element.get('content', '') or ''
            src = element.get('src', '') or ''
            with open(os.path.join(path_content, f'doc{start + i}.txt'), 'w', encoding='utf-8') as f:
                f.write(content)
            with open(os.path.join(path_html, f'html{start + i}.html'), "w", encoding="utf-8") as f:
                f.write(src)
        print("Save documents successfully")

    def load(self, file_name):
        print("Load data\n----------------------------------------------------------")
        with open(file_name, "r", encoding="utf-8") as f:
            data = json.load(f)
            self.data = data
        print("Load data successfully!")


class Parser:
    def __init__(self, tag_handlers: Optional[Dict[str, Callable]] = None):
        self.tag_handlers = tag_handlers or self._default_handlers()

    def _default_handlers(self) -> Dict[str, Callable]:
        return {
            "h1": lambda el: f"# {self._clean_text(el.get_text())}",
            "h2": lambda el: f"## {self._clean_text(el.get_text())}",
            "h3": lambda el: f"### {self._clean_text(el.get_text())}",
            "p": lambda el: self._clean_text(el.get_text()),
            "ul": self._handle_ul,
            "ol": self._handle_ol,
            "table": self._handle_table,
            "br": lambda el: "",
        }

    def parse(self, html: str) -> str:
        self.output_lines = []
        self.soup = BeautifulSoup(html, "html.parser")
        container = self.soup.body if self.soup.body else self.soup

        for elem in container.descendants:
            if not hasattr(elem, "name") or elem.name is None:
                continue
            handler = self.tag_handlers.get(elem.name)
            if handler:
                result = handler(elem)
                if isinstance(result, list):
                    self.output_lines.extend(result)
                elif isinstance(result, str):
                    self.output_lines.append(result)

        return "\n\n".join(filter(None, self.output_lines))

    def _clean_text(self, text):
        return ' '.join(text.split())

    def _handle_ul(self, ul_tag):
        return [f"- {self._clean_text(li.get_text())}" for li in ul_tag.find_all("li", recursive=False)]

    def _handle_ol(self, ol_tag):
        return [f"{i+1}. {self._clean_text(li.get_text())}" for i, li in enumerate(ol_tag.find_all("li", recursive=False))]

    def _extract_text_with_br(self, element):
        """Trích text của element, giữ xuống dòng tại <br> thành '\n'."""
        texts = []
        for item in element.descendants:
            if getattr(item, "name", None) == "br":
                texts.append('\n')
            elif isinstance(item, str):
                texts.append(item)
        return ''.join(texts).strip()

    def _handle_table(self, table_tag):
        rows = []
        for tr in table_tag.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            row = []
            for cell in cells:
                cell_text = self._extract_text_with_br(cell)
                # Markdown không cho xuống dòng trong ô, thay \n bằng dấu cách
                cell_text = ' '.join(line.strip() for line in cell_text.split('\n') if line.strip())
                cell_text = re.sub(r"-{2,}", "", cell_text)
                row.append(cell_text)
            if row:
                rows.append(row)
        return self._format_table(rows)

    def _format_table(self, rows):
        if not rows or not rows[0]:
            return ""

        # Tìm số cột lớn nhất
        max_cols = max(len(row) for row in rows)
        # Bổ sung "" cho các dòng thiếu cột
        for row in rows:
            while len(row) < max_cols:
                row.append("")

        col_widths = [max(len(cell) for cell in col) for col in zip(*rows)]
        lines = []

        header = "| " + " | ".join(cell.ljust(col_widths[i]) for i, cell in enumerate(rows[0])) + " |"
        separator = "|-" + "-|-".join('-' * col_widths[i] for i in range(len(col_widths))) + "-|"
        lines.append(header)
        lines.append(separator)

        for row in rows[1:]:
            line = "| " + " | ".join(cell.ljust(col_widths[i]) for i, cell in enumerate(row)) + " |"
            lines.append(line)

        return lines


if __name__ == "__main__":
    home_page = URL()
    home_page.crawl_links(limit=40)
    home_page.save_links("demo/crawl/data/tvpl/", "links.json")
    home_page.load('demo/crawl/data/tvpl/links.json')

    for i in range(0, 20, 20):
        home_page.crawl_docs(start=i, end=i + 20)
        home_page.save_docs("demo/crawl/data/tvpl/html/", "demo/crawl/data/tvpl/docs/", start=i)
        home_page.docs = []
