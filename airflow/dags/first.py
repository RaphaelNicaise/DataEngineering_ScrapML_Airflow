from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import mysql.connector
from bs4 import BeautifulSoup
import requests
from configparser import ConfigParser

parser = ConfigParser()
parser.read('pipeline.conf')

# Crear archivo de funcionalidades y conectarlo

db_config = {
     'host': parser['configDBMysql']['host'],
     'port': parser['configDBMysql']['port'],
     'user': parser['configDBMysql']['user'],
     'password': parser['database']['password'],
     'database': parser['database']['database']
}


def connect_to_db():
    try:
        return mysql.connector.MySQLConnection(
            **db_config
        )                 
    except mysql.connector.Error as err:
        logging.error(err)
        return None
    
def scrapear_mercado_libre(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        productos = soup.find_all('li', class_='ui-search-layout__item')
        logging.info(f'Encontrados {len(productos)} productos en la página')
        return [{'html':str(producto)} for producto in productos]
    else:
        logging.error(f'Error en la respuesta: {response.status_code}')
        return []

#PARA CORRERLO ABRIR DOS CONSOLAS, Y CORRER 
# airflow db init
# COLOCAR LAS VARIABLES DE ENTORNO
# airflow webserver --port 8080
# airflow scheduler

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['raphanicaise@gmail.com']
}

# 3 Funciones que representan la tarea del DAG
def scrape(**kwargs):
    logging.info('Scraping data')
    scraped_data = scrapear_mercado_libre('https://listado.mercadolibre.com.ar/guitarras#D[A:guitarras]')
    ti = kwargs['ti']
    ti.xcom_push(key='scraped_data', value=scraped_data) # mandas scraped_data a la siguiente tarea mediante la key 'scraped_data' y valor que tenga scraped data

def process(**kwargs):
    logging.info('Processing data')
    ti = kwargs['ti']
    data = ti.xcom_pull(key='scraped_data', task_ids='scrape') # obtenemos el valor de scraped_data que se mando al xcom en la tarea scrape
    logging.info(data)
    if not data:
        logging.error('No data disponible para procesar')
        return
    processed_data = []
    
    for producto_dict in data:
        producto_html = producto_dict['html']
        producto = BeautifulSoup(producto_html, 'html.parser')
        try:
            title_element = producto.find('h2', class_='ui-search-item__title')
            price_element = producto.find('span', class_='andes-money-amount__fraction')
            href_element = producto.find('a', class_='ui-search-item__group__element')
            
            title = title_element.text.strip() if title_element else 'Sin título'
            price = price_element.text.strip() if price_element else 'Sin precio'
            href = href_element['href'] if href_element else 'Sin enlace'
            
            processed_data.append(dict({
                'title': title, 
                'price': price, 
                'href': href
            }))
        except Exception as e:
            logging.error(e)
    ti.xcom_push(key='processed_data', value=processed_data) # mandamos procesed_data a la siguiente tarea mediante la key 'processed_data' y valor que tenga processed_data

def save(**kwargs):
    logging.info('Saving data')
    data = kwargs['ti'].xcom_pull(key='processed_data', task_ids='process') # obtenemos el valor de processed_data que se mando al xcom en la tarea process)
    try:
        cnx = connect_to_db()
        if cnx:
            cursor = cnx.cursor()
            for producto in data:
                cursor.execute('INSERT INTO guitarras (title, price, href) VALUES (%s, %s, %s)', (producto['title'], producto['price'], producto['href']))
            cnx.commit()
            cursor.close()
            cnx.close()
        else:
            logging.error('No se pudo conectar a la base de datos')    
    except mysql.connector.Error as err:
        logging.error(err)
        
dag = DAG( 
    'first',
    default_args=default_args,
    description='Tutorial de Feregrino Airflow',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example']
) # Creado un objeto Dag, el cual dentro tiene esas tareas defininidas
    
# LO MISMO QUE HACER 
# with DAG(...) as dag: funcionalidades y los >> >>

    # Cada tarea del DAG se define usando PythonOperator, que ejecuta las funciones scrape, process y save.
scrape_task = PythonOperator(
    task_id="scrape",
    python_callable=scrape,
    provide_context=True,
    dag=dag # Le pasamos el objeto dag, tambien se puede hacer con 
)
    
process_task = PythonOperator(
    task_id="process",
    python_callable=process,
    provide_context=True,
    dag=dag
)
    
save_task = PythonOperator(
    task_id="save",
    python_callable=save,
    provide_context=True,
    dag=dag
)
# Determinamos orden y dependendencias de ejecucion
scrape_task >> process_task >> save_task 
    