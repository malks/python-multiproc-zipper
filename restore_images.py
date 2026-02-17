import os,wget,requests,glob,boto3,zipfile,shutil,socket,random
from multiprocessing import Process

from botocore.exceptions import ClientError
from datetime import date
from time import sleep

#Se fizer a importação de mysql_connection (meu script q conecta no nosso banco e cria a variavel de conexão) diretamente, a variavel mydb_conn não é definida, tem que ser como abaixo
from mysql_connection import run_select,run_sql,run_select_array_ret,new_conn

#Importa modulo de Imagens
from PIL import Image
from PIL import ImageFile

resized_url="http://mktlunelli.lunenderstore.com/resizedimages/"
img_variations=[
  ".jpg",
  "-C1.jpg",
  "-C2.jpg",
  "-C3.jpg",
  "-C4.jpg",
  "-D1.jpg",
  "-D2.jpg",
  "-D3.jpg",
  "-D4.jpg",
  "-still.jpg"
]
image_formats = ("image/png", "image/jpeg", "image/jpg","binary/octet-stream")

def exists_resized(path):
    r=requests.head(path)
    if "content-type" in r.headers and r.headers["content-type"] in image_formats:
        return True
    return False

if __name__ == "__main__":
    main_conn=new_conn()
    items=run_select_array_ret("SELECT distinct systextil_notas_itens.item  FROM lepard_magento.systextil_notas_itens JOIN lepard_magento.systextil_notas ON lepard_magento.systextil_notas.numero_nota=lepard_magento.systextil_notas_itens.numero_nota AND lepard_magento.systextil_notas.serie_nota=lepard_magento.systextil_notas_itens.serie_nota LEFT JOIN lepard_magento.systextil_notas_itens_images ON lepard_magento.systextil_notas_itens.item = lepard_magento.systextil_notas_itens_images.item WHERE image IS NULL AND status='S' LIMIT 1000",main_conn)

    for item in items:
        for variation in img_variations:
            img_url = resized_url + item + variation

            if exists_resized(img_url):
                run_sql(
                    "INSERT IGNORE INTO lepard_magento.systextil_notas_itens_images (item, image, deprecated) "
                    "VALUES ('" + item + "','" + img_url + "',1)",
                    main_conn
                )
                
    main_conn.close()
