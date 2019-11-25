from requests import Session
from bs4 import BeautifulSoup as bs
from queue import Queue
from threading import Thread
import csv

class EmailHandler(Thread):
    def __init__(self, emails):
        Thread.__init__(self)
        self.queue = emails

    def run(self):
        while True:
            email = self.queue.get()
            with open('emails.csv', 'a', newline='') as f:
                writer = csv.writer(f, delimiter=',')
                writer.writerow([email])
            self.queue.task_done()

class VacanciesParser(Thread):
    def __init__(self, url, pages, session, emails):
        Thread.__init__(self)
        self.queue = pages
        self.session = session
        self.url = url
        self.emails = emails

    def run(self):
        while True:
            page = self.queue.get()
            self.parse_vacancies(page)

    def parse_vacancies(self, page):
        responde = self.session.get(self.url, params={'page': page})
        soup = bs(responde.content, 'lxml')
        links = soup.find_all('a', attrs={'data-qa': 'vacancy-serp__vacancy-title'})
        for link in links:
            s = bs(self.session.get(link['href']).content, 'lxml')
            try: 
                email = s.find('a', attrs={'data-qa': 'vacancy-contacts__email'}).text
                self.emails.put(email)
            except:
                pass

def parse_pages():
    headers = {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36',
        'accept': '*/*'
    }
    url = "https://hh.ru/search/vacancy"
    session = Session()
    session.headers = headers

    r = session.get(url)
    if r.status_code != 200:
        print('Error occurred while parsing! Check network connection.')
        return
    soup = bs(r.content, 'lxml')
    try:
        count = int(soup.find_all('a', attrs={'data-qa': 'pager-page'})[-1].text)
    except:
        count = 1

    pages = Queue()
    emails = Queue()
    for i in range(10):
        thread = VacanciesParser(url, pages, session, emails)
        thread.setDaemon(True)
        thread.start()
    handler = EmailHandler(emails)
    handler.start()

    for i in range(count):
        pages.put(i)
    emails.join()
    pages.join() 

if __name__ == '__main__':
    parse_pages()