#!/usr/bin/python3
# -*- coding: latin-1 -*-
##########DEPENDENCIAS###################################################################
#  apt-get install python3-pip
#  python3 -m pip install parse
#  python3 -m pip install mysql-connector-python
#  python3 -m pip install --upgrade pip
#  python3 -m pip install --upgrade Pillow
#  python3 -m pip install boto3
#  python3 -m pip install wget
###########################################################################################
#Importo alguns recursos necessários
import os,wget,requests,glob,boto3,zipfile,shutil
from multiprocessing import Process

from botocore.exceptions import ClientError
from datetime import date

#Se fizer a importação de mysql_connection (meu script q conecta no nosso banco e cria a variavel de conexão) diretamente, a variavel mydb_conn não é definida, tem que ser como abaixo
from mysql_connection import run_select,run_sql,run_select_array_ret,new_conn

#Importa modulo de Imagens
from PIL import Image
from PIL import ImageFile

ImageFile.LOAD_TRUNCATED_IMAGES=True

#Variáveis de trabalho
resized_url="https://marketing-lunelli.s3-sa-east-1.amazonaws.com/resizedimages/"
zip_url="https://marketing-lunelli.s3-sa-east-1.amazonaws.com/produtoimagem/"
work_dir=os.path.join(os.getcwd(),'workdir')
logos_dir=os.path.join(work_dir,'logos')
logo_position=(30,40)
img_variations=[
  ".jpg",
  "-variacao01.jpg",
  "-variacao02.jpg",
  "-C1.jpg",
  "-C2.jpg",
  "-C3.jpg",
  "-still.jpg"
]
img_urls=[
  "https://qg.lunenderstore.com/produtosb2b/",
  "https://qg.lunenderstore.com/produtos/"
]
logos_web={
  "alakazoo":"https://marketing-lunelli.s3-sa-east-1.amazonaws.com/desenv/marcas/alakazoo/logo_akz.png",
  "fico":"https://marketing-lunelli.s3-sa-east-1.amazonaws.com/desenv/marcas/fico/fico_logo.png",
  "graphene":"https://marketing-lunelli.s3-sa-east-1.amazonaws.com/desenv/marcas/graphene/logo_black_graphene.png",
  "hangar":"https://marketing-lunelli.s3-sa-east-1.amazonaws.com/desenv/marcas/hangar33/logo_hangar.png",
  "hits":"https://marketing-lunelli.s3-sa-east-1.amazonaws.com/desenv/marcas/hits/logo_black.png",
  "lezalez":"https://marketing-lunelli.s3-sa-east-1.amazonaws.com/desenv/marcas/lezalez/lezalez-black.png",
  "lunender":"https://marketing-lunelli.s3-sa-east-1.amazonaws.com/desenv/marcas/lunender/lunender.png",
  "maismulher":"https://marketing-lunelli.s3-sa-east-1.amazonaws.com/desenv/marcas/maismulher/maismulher.png"
}
logos={
  "alakazoo":os.path.join(work_dir,"logos","alakazoo.jpg"),
  "fico":os.path.join(work_dir,"logos","fico.jpg"),
  "graphene":os.path.join(work_dir,"logos","graphene.jpg"),
  "hangar":os.path.join(work_dir,"logos","hangar.jpg"),
  "hits":os.path.join(work_dir,"logos","hits.jpg"),
  "lezalez":os.path.join(work_dir,"logos","lezalez.jpg"),
  "lunender":os.path.join(work_dir,"logos","lunender.jpg"),
  "maismulher":os.path.join(work_dir,"logos","maismulher.jpg"),
}

#Link retorna imagem?
def exists(path):
   image_formats = ("image/png", "image/jpeg", "image/jpg")
   r = requests.head(path)
   if r.headers["content-type"] in image_formats:
      return True
   return False
  



#Baixamos a imagem original para depois trabalhar com ela, movê-la para a pasta de redimensionadas e zipá-la
def download_source(item,source_dir):
  for img_var in img_variations:
    if not os.path.exists(os.path.join(source_dir,item['item']+img_var)):
      if exists(img_urls[0]+item['item']+img_var):
        wget.download(img_urls[0]+item['item']+img_var,source_dir)
      elif exists(img_urls[1]+item['item']+img_var):
        wget.download(img_urls[1]+item['item']+img_var,source_dir)


#Se ja existe a imagem redimensionada, não precisa criá-la, baixamos no diretório de trabalho para zipá-la
def download_resized(item,resized_dir):
  for img in item["images"]:
    if not os.path.exists(os.path.join(resized_dir,img.split("/")[-1])):
      if exists(img):
        wget.download(img,resized_dir)


#Faz o upload dos itens redimensionados e com logo
def upload_resized(resized_dir,item):
  items_dir=os.path.join(resized_dir,item["item"]+"*")
  files=glob.glob(items_dir)
  s3_client = boto3.client('s3')

  for file_name in files:
    if not exists(resized_url+file_name.split("/")[-1]):
      s3_client.upload_file(file_name, 'marketing-lunelli', "resizedimages/"+file_name.split("/")[-1])
    item["images"].append(resized_url+file_name.split("/")[-1])


#Retorna o link da logo de cada marca, é assim porque a marca no banco não vem sempre certo, tem Lunender Jeans, basicos, etc
def get_marca_logo(item):
  if item["marca"].lower().find('lunender')>=0:
    return logos["lunender"]
  elif item["marca"].lower().find('lez')>=0:
    return logos["lezalez"]
  elif item["marca"].lower().find('alakazoo')>=0:
    return logos["alakazoo"]
  elif item["marca"].lower().find('hangar')>=0:
    return logos["hangar"]
  elif item["marca"].lower().find('graphene')>=0:
    return logos["graphene"]
  elif item["marca"].lower().find('hits')>=0:
    return logos["hits"]
  elif item["marca"].lower().find('mais mulher')>=0:
    return logos["maismulher"]
  elif item["marca"].lower().find('fico')>=0:
    return logos["fico"]
  else:
    return False


#Redimensiona a imagem e coloca a logo no canto
def reduction_and_stamping(item,source_dir,resized_dir):
  #Pego link para a logo, abro e jogo dentro da varivel logo_image
  proc_logo_url=get_marca_logo(item)
  if not proc_logo_url==False:
    logo_image=Image.open(proc_logo_url)
  items_dir=os.path.join(source_dir,item["item"]+"*")
  files=glob.glob(items_dir)

  for img in files:
    if not os.path.exists(os.path.join(resized_dir,img.split("/")[-1])):
      source_image=Image.open(img)
      work_image=source_image.copy()
      work_image.thumbnail((1000,1000))
      if not proc_logo_url==False:
        work_image.paste(logo_image,logo_position)
      work_image.save(os.path.join(resized_dir,img.split("/")[-1]))


def zipem(nota_dir,resized_dir,item,nota):
  files=glob.glob(os.path.join(resized_dir,"*.jpg"))
  current_date=date.today().strftime("%Y%m%d")
  s3_client = boto3.client('s3')

  nota_zip_file=os.path.join(nota_dir,current_date+item['numero_nota']+item['serie_nota']+".zip")

  with zipfile.ZipFile(nota_zip_file, 'w') as my_zip:
    for file_name in files:
      my_zip.write(file_name,file_name.split("/")[-1],compress_type=zipfile.ZIP_DEFLATED)

  if not exists(zip_url+nota_zip_file.split("/")[-1]):
    s3_client.upload_file(nota_zip_file, 'marketing-lunelli', "produtoimagem/"+nota_zip_file.split("/")[-1])
  nota["nome_arquivo"]=nota_zip_file.split("/")[-1]

    
#Executa processos em cima de notas e itens
def ready_go(nota):
  proc_conn=new_conn()
  for item in nota["items"]:
    nota_dir=item['numero_nota']+item['serie_nota']
    full_dir=os.path.join(work_dir,nota_dir)
    source_dir=os.path.join(full_dir,"sources")
    resized_dir=os.path.join(full_dir,"resized")

    if not os.path.exists(full_dir):
      os.mkdir(full_dir)

    if not os.path.exists(source_dir):
      os.mkdir(source_dir)

    if not os.path.exists(resized_dir):
      os.mkdir(resized_dir)

    if not len(item['images'])>0:
      download_source(item,source_dir)
      reduction_and_stamping(item,source_dir,resized_dir)
    else:
      download_resized(item,resized_dir)
    
    upload_resized(resized_dir,item)
    zipem(full_dir,resized_dir,item,nota)
    for img in item["images"]:
      run_sql("INSERT IGNORE INTO lepard_magento.systextil_notas_itens_images (item,image) values('"+item["item"]+"','"+img+"')",proc_conn)

  #Atualiza banco para depois atualizar o systextil
  run_sql("UPDATE lepard_magento.systextil_notas SET status='S',nome_arquivo='"+nota["nome_arquivo"]+"' WHERE numero_nota='"+nota["numero_nota"]+"' and serie_nota='"+nota["serie_nota"]+"'",proc_conn)

  #Remove diretório da nota e seu conteúdo pra não manter sujeira no disco
  shutil.rmtree(full_dir)
  
      


#Obtém os itens das notas que foram importadas pro nosso banco
def get_items(nota,serie,conn):
  ret=run_select("SELECT * FROM lepard_magento.systextil_notas_itens WHERE numero_nota='"+nota+"' and serie_nota='"+serie+"'",conn)
  for i in ret:
    i["images"]=run_select_array_ret("SELECT image FROM lepard_magento.systextil_notas_itens_images WHERE item='"+i["item"]+"'",conn)
  return ret

#Para rodar na execução do python
if __name__ == "__main__":
  main_conn=new_conn()
  running=run_select("SELECT numero_nota,serie_nota FROM lepard_magento.systextil_notas where status='R'",main_conn)
  running=len(running)
  max_threads=8-running

  if running>7:
    quit()
  run_sql("DELETE FROM lepard_magento.systextil_notas_itens_images WHERE date_format(created_at,'%Y-%m-%d') < date_format(date_sub(NOW(), INTERVAL 1 MONTH),'%Y-%m-%d')",main_conn)
  #Pega as notas importadas
  notas=run_select("SELECT numero_nota,serie_nota,status,nome_arquivo FROM lepard_magento.systextil_notas where status='P' limit "+str(max_threads),main_conn)

  for nota in notas:
    nota['items']=get_items(nota['numero_nota'],nota['serie_nota'],main_conn)
    run_sql("UPDATE lepard_magento.systextil_notas SET status='R' where numero_nota='"+nota["numero_nota"]+"' and serie_nota='"+nota["serie_nota"]+"'",main_conn)

  while len(notas)>0:
    jobs=[]
    for i in range(0,max_threads):
      if(len(notas)>0):
        nota=notas.pop(0)
        thread=Process(target=ready_go,args=(nota,))
        jobs.append(thread)
    
    for j in jobs:
      j.start()
    
    for j in jobs:
      j.join()