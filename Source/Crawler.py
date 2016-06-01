#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mysql.connector import (connection)
import urllib
import re
import sys

sPrimaryURL = "http://www.kabum.com.br"
aProductLinks = []

from mysql.connector import errorcode

try:
    cnx = connection.MySQLConnection(user='root', password='YjGmnSPQro',
                                     host='127.0.0.1',
                                     database='Crawler')
    
    cursor = cnx.cursor()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Acesso negado (possivelmente login ou senha incorretos)")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database não existe")
    else:
        print(err)
else:
    print("Conectado a Database.")

print("Começando a buscar produtos em: " + sPrimaryURL)

f = urllib.urlopen(sPrimaryURL)
url = f.read()

urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', url)

print(str(len(urls)) + " urls encontradas no index")

for link in urls:
    if (link not in aProductLinks and ".jpg" not in link and ".png" not in link and "kabum.com.br" in link):
        aProductLinks.append(link)

auto_stop = 0
count_products = 0
all_count = 0

preco = []
titulo = []

for i in aProductLinks:
    preco = []
    titulo = []
    all_count = all_count + 1
    
    f = urllib.urlopen(i)
    url = f.read()
    
    if "produto/" in i:
        titulo = re.findall(r'<h1 class="titulo_det">(.*?)</h1', url)
        preco = re.findall(r'<span class="preco_desconto_avista-cm">(.*?)</span', url)
        
    urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', url)
    
    for link in urls:
        if (link not in aProductLinks and ".jpg" not in link and ".png" not in link and "kabum.com.br" in link):
            aProductLinks.append(link)
    
    if len(preco) < 1:
        preco = re.findall(r'<span style="font-size: 35px; letter-spacing: -2px;"><strong>(.*?)</strong', url)
    
    
    sys.stdout.write("\rLinks escaneados: " + str(count_products) + "(" + str(all_count) + "/" + str(len(aProductLinks)) + ")")
    sys.stdout.flush()
    
    if len(titulo) < 1 or len(preco) < 1:
        continue
    
    add_product = ("INSERT INTO `Crawler`.`Kabum_Links` "
               "(`Prod_link`, `Pord_price`, `Prod_name`, `idPichau_DB`) "
               "VALUES (%s, %s, %s, %s)")

    data_product = (str(i), str(preco[0]), str(titulo[0]), '0')
    
    cursor.execute(add_product, data_product)
    
    auto_stop = auto_stop+1
    count_products = count_products + 1
    #print("Produto: " + titulo[0] + " Preço: " + preco[0])
    
    if(auto_stop >= 25):
        cnx.commit()
        auto_stop = 0

cnx.commit()    

cursor.close()
cnx.close()