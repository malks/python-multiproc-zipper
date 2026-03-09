import os,sys,wget,requests,glob,boto3,zipfile,shutil,socket,random
from multiprocessing import Process

from botocore.exceptions import ClientError
from datetime import date
from time import sleep

#Se fizer a importação de mysql_connection (meu script q conecta no nosso banco e cria a variavel de conexão) diretamente, a variavel mydb_conn não é definida, tem que ser como abaixo
from mysql_connection import run_select,run_sql,run_select_array_ret,new_conn

#Importa modulo de Imagens
from PIL import Image
from PIL import ImageFile

image_url="https://d1ik73g39xfbe.cloudfront.net/front/thumbs/custom/"

image_formats = ("image/png", "image/jpeg", "image/jpg","binary/octet-stream")

def exists_image(path):
    try:
        r = requests.head(path, timeout=5)
        if r.status_code == 200 and "content-type" in r.headers:
            if r.headers["content-type"].lower() in image_formats:
                return True
    except requests.RequestException:
        pass
    return False

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Uso: python script.py <arquivo>")
        sys.exit(1)

    file_path = sys.argv[1]
    file_name = os.path.basename(file_path)

    img_url = image_url + file_name

    if exists_image(img_url):
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Arquivo removido: {file_path}")
        else:
            print(f"Arquivo não encontrado localmente: {file_path}")
    else:
        print(f"Arquivo não encontrado no cloudfront: {file_path}")