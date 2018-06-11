from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import urllib
import random
import requests
import time
from pymongo import MongoClient
from socket import timeout


ua = UserAgent() # From here we generate a random user agent
proxies = list() # Will contain proxies [ip, port]

# Main function
def getFreeProxies():
  global proxies
  # Retrieve latest proxies
  proxies_req = Request('https://www.sslproxies.org/')
  proxies_req.add_header('User-Agent',  'Mozilla/5.0')
  proxies_doc = urlopen(proxies_req).read().decode('utf8')
  soup = BeautifulSoup(proxies_doc, 'html.parser')
  proxies_table = soup.find(id='proxylisttable')
  # Save proxies in the array
  for row in proxies_table.tbody.find_all('tr'):
    if row.find_all('td')[6].string == 'yes':
        proxies.append({
           'ip':   row.find_all('td')[0].string,
              'port': row.find_all('td')[1].string,
              'country': row.find_all('td')[3].string,
              'anonymity': row.find_all('td')[4].string,
              'https': row.find_all('td')[6].string,
              'lascheck': row.find_all('td')[7].string
        })
  

def getRandomProxy():
    global proxies
    if countProxies()==0:
        proxies = getFreeProxies()
    proxy_index = random_proxy()
    proxy = proxies[proxy_index]
    return proxy_index, proxy

  
def deleteProxyFromList(proxy_index):
    global  proxies
    del proxies[proxy_index]

def random_proxy():
    global  proxies
    return random.randint(0, len(proxies) - 1
)
def countProxies():
    global proxies
    return len(proxies) 
def checkIfProxyWorking():    
    index, proxy = getRandomProxy()
    req = Request('http://icanhazip.com')
    req.set_proxy(proxy['ip'] + ':' + proxy['port'], 'http')
    try:
      i = urlopen(req).read().decode('utf8')
      print('current proxy', i)
      return True,index, proxy
    except: # If error, delete this proxy and find another one
      deleteProxyFromList(index)
      return False, 0 , '0 '

def main():
    getFreeProxies()
    for i in range(158084,  200000):
        url = 'https://connect.data.com/company/view/' + str(i)
        isUrlDone = False
        while(isUrlDone == False):
            index, proxy = getRandomProxy()
            print('working proxi',proxy['ip'] + ':' + proxy['port'] )
            req = Request(url)
            req.set_proxy(proxy['ip'] + ':' + proxy['port'], 'https')
            req.add_header('User-Agent', ua.random)
            try:
                conn = urlopen(req,timeout=20)
                print('connected')
            except urllib.error.HTTPError as e:
                isUrlDone = True
                print('HTTPError: {}'.format(e.code))
            except urllib.error.URLError as e:
                print('URLError: {}'.format(e.reason))
                deleteProxyFromList(index)
            except timeout:
                print('socket timed out - URL %s', url)
            else:
                html_text = conn.read().decode('utf8')
                dict_company = search(html_text)
                dict_company['proxyip'] = proxy['ip']  + ":" + proxy['port']
                dict_company['i'] = i
                dict_company['url'] = conn.geturl()
                insert_mongodb(dict_company)
                print('mongo inserted dic')
                isUrlDone = True
                print(conn.geturl(), i, 'done', '200')
                
def search(Page_content):
    json_dict = {}
    soup = BeautifulSoup(Page_content, 'html.parser')
    total_length = len(soup.find_all(class_= "seo-company-data"))
    column_names = ['Name of company','Website of company', 'Headquters of company', 'Phone number', 'Industery Type', 'Employee_size', 'Revenue of company']
    for index in range(total_length-1):
        fetched_data = soup.find_all(class_= "seo-company-data")[index].get_text().strip()
        json_dict[column_names[index]] = fetched_data.replace('\r\n','').replace('\xa0map','')
    return json_dict

def insert_mongodb(dic):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['datacom']
    collection = db['companies']
    collection.insert_one(dic)   

if __name__ == "__main__":
    main()             
