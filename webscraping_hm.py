import re
import os
import math
import logging
import sqlite3
import requests

import pandas as pd
import numpy  as np

from datetime   import datetime 
from sqlalchemy import create_engine
from bs4 import BeautifulSoup


# Collect data ( coleta os 36 prodcts da vitrini)

# Parameters
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

# url do site
url = 'https://www2.hm.com/en_us/men/products/jeans.html'


def data_collection( url, headers ):

    # Request to URL
    page = requests.get( url, headers=headers )
    
    # Beautifullsoup
    soup = BeautifulSoup( page.text, 'html.parser' )
    
    # ===================  Product Data  =================================
    products = soup.find('ul', class_= 'products-listing small')
    product_list = products.find_all('article', class_ = 'hm-product-item')
    
    # Product_id
    product_list[0].get('data-articlecode')
    product_id = [i.get('data-articlecode') for i in product_list]
    
    # Product_category
    product_list[0].get('data-category')
    product_category = [i.get('data-category') for i in product_list]
    
    # product_name
    product_list = products.find_all('a', class_= 'item-link' )
    product_list[0].get('title')
    product_name = [i.get('title') for i in product_list]
    
    # product_price
    product_list = products.find_all('span', class_='price regular')
    product_price = [i.get_text() for i in product_list]
    
    
    data = pd.DataFrame([ product_id, product_category, product_name, product_price ]).T
    data.columns = ['product_id','product_category', 'product_name', 'product_price']
    
    # scrapy datetime
    data['scrapy_datetime'] = datetime.now().strftime( '%Y-%m-%d %H:%M:%S:')
    return data


# Data Collection by Product ( coleta dos prod sxpecifico entra em cada um e faz as rspagem )
def data_collection_by_product( data, headers):
    
    # Dicionário que diz para o endpoint que o usuário é um Chrome (Browser)
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    
    df_compositions = pd.DataFrame()
    
    # Cria uma lista vazia para adicionar as colunas que não foram coletadas anteriormente, caso um novo produto tenha alguma característica não capturada antes
    aux = []
    
    # Suponha que você tenha um DataFrame chamado "data" com informações dos produtos, como 'product_id'
    for i in range(len(data)):
        URL = 'https://www2.hm.com/en_us/productpage.' + data.loc[i, 'product_id'] + '.html' 
        logger.debug( 'Product: %s', URL )  # Correção na formatação da URL
    
        # Requisita essa URL do servidor do site H&M
        page = requests.get(URL, headers=headers)
    
        # Parâmetro de entrada para o BeautifulSoup
        soup = BeautifulSoup(page.text, 'html.parser')
    
        # ========================== Color Name ===============================
        
        # Product_color
        product_list = soup.find_all('a', class_='filter-option miniature')
        product_color = [i.get('data-color') for i in product_list]
    
        # Product_id
        product_id = [i.get('data-articlecode') for i in product_list]
        
        # Criando o DataFrame
        df_color = pd.DataFrame([ product_color,  product_id]).T
        df_color.columns = ['product_color', 'product_id']
    
        for j in range(len(df_color)):
            url = 'https://www2.hm.com/en_us/productpage.' + df_color.loc[j, 'product_id'] + '.html' 
            logger.debug( 'Color: %s', url )
            
            page = requests.get(url, headers=headers)
            soup = BeautifulSoup(page.text, 'html.parser')
            
            # ===============================  =======================================
            product_elements = soup.find_all('div', class_='content pdp-text pdp-content')
            
            # Inicialize uma lista para armazenar as informações de todos os produtos
            all_products_info = []
            
            for product_element in product_elements:
                product_info = {}
                # =============================== product_name =======================================    
                product_name_element = product_element.find('h1', class_='primary product-item-headline')
                if product_name_element:
                    product_name = product_name_element.text.strip()
                    product_info['product_name'] = product_name
                
                # =============================== Product Price  =====================================
                price_tag = soup.find('span', class_='price-value')
                if price_tag:
                    price = price_tag.get_text(strip=True)
                    product_info['price'] = price
                
                # =============================== Composition =======================================
                # Extrair informações específicas para cada produto
                fit_element = product_element.find('dt', string='Fit')
                if fit_element:
                    fit = fit_element.find_next('dd').text.strip()
                    product_info['fit'] = fit
                
                size_element = product_element.find('dt', string='Size')
                if size_element:
                    size = size_element.find_next('dd').text.strip()
                    product_info['size'] = size
                
                composition_element = product_element.find('h3', string='Composition')
                if composition_element:
                    composition = composition_element.find_next('ul').text.strip()
                    product_info['composition'] = composition.replace('\n', '')
    
                article_number_element = product_element.find('div', string=re.compile(r'Article number\d+'))
                if article_number_element:
                    article_number_text = article_number_element.text.strip()
                    product_id = re.search(r'\d+', article_number_text).group()
                    product_info['product_id'] = product_id
                    product_info['product_color'] = df_color.loc[j, 'product_color']
                    
                
                # Adicionar as informações do produto à lista
                all_products_info.append(product_info)
    
            # Criar o DataFrame
            df_composition = pd.DataFrame(all_products_info, columns=['product_name', 'price', 'fit', 'size', 'composition', 'product_id', 'product_color'])
    
            # Garantir o mesmo número de colunas
            df_composition = df_composition[['product_name', 'price', 'fit', 'size', 'composition', 'product_id', 'product_color']]
            
            # Atualizar a lista auxiliar com novas colunas
            aux = aux + df_composition.columns.tolist()
    
            # ========================== df_color + df_composition ==================================
            df_compositions = pd.concat([df_compositions, df_composition], axis=0)
    
    df_compositions['style_id'] = df_compositions['product_id'].apply(lambda x: x[:-3])
    df_compositions['color_id'] = df_compositions['product_id'].apply(lambda x: x[-3:])
    
    # Definir a coluna 'scrapy_datetime' com a data e hora atual
    df_compositions['scrapy_datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return df_compositions

# Data Cleaning (* limpeza e separacao dados:)

def data_cleaning( data_product ):
    ## === transfommacao das col  ====
    
    df_data = data_product.dropna(subset=['product_id'])
    
    # product_name
    #df_data['product_name'] = df_data['product_name'].apply(lambda x: x.replace(' ', '_').lower())
    
    # product_price - Remover o símbolo '$' e converter para float
    df_data['price'] = df_data['price'].apply(lambda x: x.replace('$', '') if isinstance(x, str) else x ).astype(float)
    
    # scrapy_datetime
    # df_data['scrapy_datetime'] = pd.to_datetime(df_data['scrapy_datetime'], format='%Y-%m-%d %H:%M:%S:')
    
    # Color Name
    df_data['product_color'] = df_data['product_color'].apply(lambda x: x.replace(' ', '_').replace('/', '_').lower() if isinstance(x, str) else x)
    #
    # Fit
    df_data['fit'] = df_data['fit'].apply(lambda x: x.replace(' ', '_').lower() if isinstance(x, str) else x)
    
    # Size
    df_data['size'] = df_data['size'].apply(lambda x: x.replace(' ', '_').lower() if isinstance(x, str) else x)
    #
    # size_number - Extraido números da coluna 'size'
    df_data['size_number'] = df_data['size'].apply(lambda x: re.search('\d{3}cm', x).group(0) if isinstance(x, str) and re.search('\d{3}cm', x) else x)
    df_data['size_number'] = df_data['size_number'].apply( lambda x: re.search('\d+', x).group(0) if pd.notnull( x ) else x )
    #
    # size_model - Extraido números da coluna 'size'
    df_data['size_model'] = df_data['size'].apply( lambda x: re.search('\d+/\\d+', x).group(0) if isinstance(x, str) and re.search('\d+/\\d+', x) else x  )
    #
    # ========================== brack composition by comma =================================
    #df1 = df_data['composition'].str.split(', ', expand=True).reset_index(drop=True) # onde tiver , quebro e tranforma em uma nova linha
    
    # Dividir por vírgulas, aspas e o símbolo de %
    df1 = df_data['composition'].str.split(r', |\'|%', expand=True).reset_index(drop=True)
    
    # Substitua 'Shell' e 'Pocket lining' por uma string vazia na coluna 'composition'
    df1[0] = df1[0].str.replace('Shell', '').str.replace('Pocket lining', '')
    df1[1] = df1[1].str.replace('Shell', '').str.replace('Pocket lining', '').str.replace('Pocket', '').str.replace('Lining', '')
    df1[2] = df1[2].str.replace('Shell', '').str.replace('Pocket lining', '')
    df1[3] = df1[3].str.replace('Pocket lining', '').str.replace('Pocket', '').str.replace('Lining', '')
    df1[5] = df1[5].str.replace('Pocket lining', '')
    
    #criando um dataframe vazio do tamanho de df_data para alocar as colunas em ordem
    df_ref = pd.DataFrame( index=np.arange( len( df_data ) ), columns=['cotton', 'Polyester','Spandex', 'Elastomultiester'] )
    
    # Use expressões regulares para extrair columns=['Cotton', 'Polyester','Spandex', 'Elastomultiester'] em colunas separadas
    
    # ======================================== DF Cotton =====================================================
    
    # Usando expressão regular para extrair "Cotton" da coluna 0
    df_cotton_0 = df1.loc[df1[0].str.contains('Cotton', na=True), 0] # coleto dentro das minhas col[0] quem sao meus cotton = true/false = pass um loc e sel so a col 0
    df_cotton_0.name = 'cotton'
    # Usando expressão regular para extrair "Cotton" da coluna 1
    df_cotton_1 = df1.loc[df1[1].str.contains('Cotton', na=True), 1]
    df_cotton_1.name = 'cotton'
    # Usando expressão regular para extrair "Cotton" da coluna 3
    df_cotton_3 = df1.loc[df1[3].str.contains('Cotton', na=True), 3]
    df_cotton_3.name = 'cotton'
    # Usando expressão regular para extrair "Cotton" da coluna 5
    df_cotton_5 = df1.loc[df1[5].str.contains('Cotton', na=True), 5]
    df_cotton_5.name = 'cotton'
    # Usando expressão regular para extrair "Cotton" da coluna 7
    df_cotton_7 = df1.loc[df1[7].str.contains('Cotton', na=True), 7]
    df_cotton_7.name = 'cotton'
    
    # Use o método concat para combiná-los ao longo do eixo das colunas (axis=1)
    df_cotton = pd.concat( [df_cotton_0, df_cotton_1, df_cotton_3, df_cotton_5, df_cotton_7], axis=0 )
    df_ref.reset_index(drop=True, inplace=True)
    df_cotton.reset_index(drop=True, inplace=True)
    df_ref = pd.concat([df_ref, df_cotton], axis=1)
    
    # ======================================== DF Polister =====================================================
    df_poly_1 = df1.loc[df1[1].str.contains('Polyester', na=True), 1]
    df_poly_1.name = 'polyester'
    df_poly_3 = df1.loc[df1[3].str.contains('Polyester', na=True), 3]
    df_poly_3.name = 'polyester'
    
    # Combine
    df_ref.reset_index(drop=True, inplace=True)
    df_poly = df_poly_1.combine_first( df_poly_3 )
    df_ref = pd.concat([df_ref, df_poly], axis=1)
    df_poly.reset_index(drop=True, inplace=True)
    
    # ======================================== DF Spandex =====================================================
    df_spandex_2 = df1.loc[df1[2].str.contains('Spandex', na=True), 2]
    df_spandex_2.name = 'spandex'
    
    df_spandex_4 = df1.loc[df1[4].str.contains('Spandex', na=True), 4]
    df_spandex_4.name = 'spandex'
    
    df_spandex_6 = df1.loc[df1[6].str.contains('Spandex', na=True), 6]
    df_spandex_6.name = 'spandex'

    df_spandex = pd.concat([ df_spandex_2, df_spandex_4, df_spandex_6 ], axis=0)
    
    df_ref.reset_index(drop=True, inplace=True)
    df_spandex.reset_index(drop=True, inplace=True)
    df_ref = pd.concat([df_ref, df_spandex], axis=1)
    
    # ============= DF Elastomultiester ===================
    
    df_elastomultiester_2 = df1.loc[df1[2].str.contains( 'Elastomultiester', na=True), 2]
    df_elastomultiester_2.name = 'elastomultiester' 
    
    df_elastomultiester_4 = df1.loc[df1[4].str.contains( 'Elastomultiester', na=True ),4]
    df_elastomultiester_4.name = 'elastomultiester'
    
    # Combine
    df_elastomultiester = df_elastomultiester_2.combine_first(df_elastomultiester_4)
    
    df_ref = pd.concat( [ df_ref, df_elastomultiester], axis=1 )
    
    # =============== DF 'Rayon =================
    
    df_rayon = df1.loc[ df1[2].str.contains('Rayon', na=True), 2 ]
    df_rayon.name = 'Rayon'
    
    df_ref = pd.concat( [ df_ref, df_rayon], axis=1).reset_index()
    
    df_ref = df_ref.iloc[ :, ~df_ref.columns.duplicated( keep='last' ) ].reset_index(drop=True)
    
    # join of combine with product
    df_aux = pd.concat( [df_data['product_id'].reset_index(drop=True), df_ref ], axis=1)
    
    # cotton
    df_aux['cotton'] = df_aux['cotton'].apply( lambda x: int( re.search( '\d+', x ).group(0) ) / 100 if pd.notnull( x ) else x )
    # Polyester
    df_aux['polyester'] = df_aux['polyester'].apply( lambda x: int( re.search( '\d+', x ).group(0) ) / 100 if pd.notnull( x ) else x )
    #spandex.
    df_aux['spandex'] = df_aux['spandex'].apply( lambda x: int( re.search('\d+', x).group(0) ) /100 if pd.notnull(x) else x )
    # Elastomultiester
    df_aux['Elastomultiester'] = df_aux['Elastomultiester'].apply( lambda x: int(re.search('\d+', x).group(0))/ 100 if pd.notnull( x ) else x)
    # Rayon
    df_aux['Rayon'] = df_aux['Rayon'].apply( lambda x: int( re.search('\d+', x).group(0) ) /100 if pd.notnull(x) else x )
    
    # final join
    df_aux = df_aux.groupby( 'product_id' ).max().reset_index().fillna( 0 )
    df_data = pd.merge( df_data,  df_aux, on='product_id', how='left' )
    
    # drop columns
    df_data = df_data.drop(columns=['size', 'composition', 'index',	'Polyester', 'Spandex',	'Elastomultiester'], axis=1)
    
      # drop duplicate
    df_data = df_data.drop_duplicates()                                                      

    return df_data


# Data Insert
def data_insert( df_data):
    # RENOMEA AS COLUNAS E ORDENA 
    data_insert = df_data[[
        'product_id',
        'style_id',
        'color_id',
        'product_name', 
        'product_color',
        'fit', 
        'price',
        'size_number',
        'size_model',
        'cotton',
        'polyester',
        'spandex',
        'elastomultiester',
        'Rayon',
        'scrapy_datetime'
    ]]
    
    # creat  Database connect
    conn = create_engine('sqlite:///database_hm.sqlite', echo=False)
    
    # Data insurt
    data_insert.to_sql('vitrine', con=conn, if_exists='append', index=False)

    return None

if __name__ =='__main__':
      
    # logging
    path = '/Users/hallanmiranda/Documents/repos/ds_dev_webscraping'

    if not os.path.exists( path + 'Logs'):
        os.makedirs( path + 'Logs')

    logging.basicConfig(
        filename = path + 'Logs/webscraping_hm.log',
        level = logging.DEBUG,
        format = '%(asctime)s - %(lavelname)s - %(name)s - %(message)s',
        datefmt = '%Y-%m-%d %H:M:S'
        )    
    logger = logging.getLogger('webscraping_hm')

    # Parameters and costants
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

    # url do site
    url = 'https://www2.hm.com/en_us/men/products/jeans.html'

    # Data collection
    data = data_collection( url, headers )
    logger.info( 'data collect done')  
    # Data coletion by product
    data_product = data_collection_by_product( data, headers )
    logger.info( 'Data collection by product  done' )      
    # Data cleaning
    data_product_cleaning = data_cleaning ( data_product )
    logger.info( 'Data product cleaned done' )             
    # Data insertion
    data_insert( data_product_cleaning )
    logger.info( 'Data insertion done' )     














