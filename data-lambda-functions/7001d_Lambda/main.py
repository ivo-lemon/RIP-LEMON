import paramiko 
from pathlib import Path
from rarfile import RarFile
import rarfile
import os 
import datetime 
import pandas as pd 
from datetime import timedelta
import boto3
from io import StringIO 
import json 
import logging
import patoolib

logger = logging.Logger("main")
formatter = logging.Formatter("%(asctime)s %(message)s")
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger.addHandler(handler)



def get_secret():
    secret_name = os.getenv("CREDENTIALS_SECRET_NAME")
    region_name = os.getenv("AWS_REGION")
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    credentials = json.loads(get_secret_value_response['SecretString'])
    return credentials["user"], credentials["password"]

# CONEXION AL SFTP

def get_files_from_sftp():
    user, password = get_secret()
    logger.info("Connecting to STP 172.17.1.1...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    transport = paramiko.Transport('172.17.1.1', 22)
    transport.connect(None, user, password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    files_on_source = sftp.listdir('Out')
    yesterday_filter = (datetime.datetime.now()).strftime("%Y%m%d")
    logger.info(f"Im going to filter files from {yesterday_filter}")
    filtered_files = [x for x in files_on_source if x.find(yesterday_filter) != -1]
    out = []
    for file in filtered_files:
        logger.info(f"Getting file {file}")
        sftp.get(remotepath=f"Out/{file}" , localpath= f"/tmp/{file}")
        out.append(file)
    return out


def extract_all(files):
    for f in files:
        if ('T7001') in f:
            if not os.path.exists(f"/tmp/{f}"):
                logger.warn(f"File {f} does not exists. Skipping")
                continue
            rar = rarfile.RarFile(f"/tmp/{f}")
            rar.extractall('/tmp/')
            os.remove(f"/tmp/{f}")
            logger.info(f"File{f} successfully extracted")

def get_t7001d_file():
    out = [x for x in os.listdir("/tmp/") if x.startswith("T7001D")]
    if len(out) > 1:
        raise Exception(f"I got more than one T7001D's files. {out}")
    if len(out) == 0:
        raise Exception(f"I didn't find any file for T7001D format")
    return out[0]

def clean_txt (file):
    logger.info(f"Processing {file}")
    df1 = pd.read_csv(f'/tmp/{file}',encoding='latin-1',delimiter = '\t')
    df1 = df1[:-1]

    # GENERO NUEVA COLUMNA DE AYUDA
    df1['Columna_ayuda'] = df1.apply(lambda x:x [0:])
    # CREOR COLUMNAS SEGUN EL LAYOUT

    df1['Nro. de Cuenta'] = df1['Columna_ayuda'].str.slice(start=1, stop=11)
    df1['Nº  de Tarjeta'] = df1['Columna_ayuda'].str.slice(start=27, stop=31)
    df1['Fecha  Procesamiento Hora:Min:Seg'] = pd.to_datetime(df1['Columna_ayuda'].str.slice(start=37, stop=51),format='%d%m%Y%H%M%S')
    df1['Importe'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=51, stop=75).str.lstrip("0"))/100
    df1['AutoMone'] = df1['Columna_ayuda'].str.slice(start=78, stop=81)
    df1['Plan'] = df1['Columna_ayuda'].str.slice(start=81, stop=99)
    df1['Código de Autorización'] = df1['Columna_ayuda'].str.slice(start=102, stop=112)
    df1['AutoForzada'] = df1['Columna_ayuda'].str.slice(start=112, stop=113)
    df1['AutoReverFlag'] = df1['Columna_ayuda'].str.slice(start=113, stop=114)
    df1['AutoAutoDebi'] = df1['Columna_ayuda'].str.slice(start=114, stop=115)
    df1['Nº  de Comercio'] = df1['Columna_ayuda'].str.slice(start=115, stop=130)
    df1['Nombre del Comercio'] = df1['Columna_ayuda'].str.slice(start=130, stop=170).str.replace("'","")
    df1['Estado'] = df1['Columna_ayuda'].str.slice(start=170, stop=179)
    df1['Rechazo'] = df1['Columna_ayuda'].str.slice(start=183, stop=263)
    df1['ICA'] = df1['Columna_ayuda'].str.slice(start=263, stop=274)
    df1['MCC'] = df1['Columna_ayuda'].str.slice(start=274, stop=278)
    df1['AutoMoneOriISO'] = df1['Columna_ayuda'].str.slice(start=379, stop=382)
    df1['Importe original autoriz'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=382, stop=395).str.lstrip("0"))/100
    df1['Importe autoriz convertido'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=395, stop=408).str.lstrip("0"))/100
    df1['Signo Importe autoriz convertido'] = df1['Columna_ayuda'].str.slice(start=408, stop=409)
    df1['Importe RG4240'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=561, stop=574).str.lstrip("0"))/100
    df1['Signo Importe RG4240'] = df1['Columna_ayuda'].str.slice(start=574, stop=575)
    df1['Importe IIBB RG4240'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=575, stop=588).str.lstrip("0"))/100
    df1['Signo Importe IIBB RG4240'] = df1['Columna_ayuda'].str.slice(start=588, stop=589)
    df1['Provincia IIBB RG4240'] = df1['Columna_ayuda'].str.slice(start=589, stop=591)
    df1['Importe IMPUESTO LEY PAIS'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=591, stop=604).str.lstrip("0"))/100
    df1['Signo Importe IMPUESTO LEY PAIS'] = df1['Columna_ayuda'].str.slice(start=604, stop=605)
    df1['Fecha Estado  Hora:Min:Seg'] = pd.to_datetime(df1['Columna_ayuda'].str.slice(start=612, stop=626),format='%d%m%Y%H%M%S', errors='coerce')
    df1['Importe RG4815'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=626, stop=639).str.lstrip("0"))/100
    df1['Signo Importe RG4815'] = df1['Columna_ayuda'].str.slice(start=639, stop=640)
    df1['Cuenta externa'] = df1['Columna_ayuda'].str.slice(start=900, stop=1200)

    df1['Request id'] = df1['Columna_ayuda'].str.slice(start=1200, stop=1250)
    #df1['Request id'] = df1['Columna_ayuda'].str.slice(start=1455, stop=1500)

    df1['Tipo de registro'] = df1['Columna_ayuda'].str.slice(start=0, stop=1)
    df1['Adicional Nº'] = df1['Columna_ayuda'].str.slice(start=11, stop=15)
    df1['Tipo de Moneda'] = df1['Columna_ayuda'].str.slice(start=75, stop=78)
    df1['Cuotas'] = df1['Columna_ayuda'].str.slice(start=99, stop=102)
    df1['Relacionada'] = df1['Columna_ayuda'].str.slice(start=179, stop=181)
    df1['Origen'] = df1['Columna_ayuda'].str.slice(start=181, stop=183)
    df1['TCC'] = df1['Columna_ayuda'].str.slice(start=278, stop=279)
    df1['Código Regla de Fraude'] = df1['Columna_ayuda'].str.slice(start=279, stop=289)
    df1['Descripción Regla de Fraude'] = df1['Columna_ayuda'].str.slice(start=289, stop=339)
    df1['Modeo de Entrada'] = df1['Columna_ayuda'].str.slice(start=339, stop=341)
    df1['Terminal POS'] = df1['Columna_ayuda'].str.slice(start=341, stop=357)
    df1['Estado1'] = df1['Columna_ayuda'].str.slice(start=170, stop=179)
    df1['STANDIN'] = df1['Columna_ayuda'].str.slice(start=377, stop=379)
    df1['Filler'] = df1['Columna_ayuda'].str.slice(start=640, stop=900)
    df1['USUARIO'] = df1['Columna_ayuda'].str.slice(start=357, stop=377)
    df1['Total de Importe cargos'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=409, stop=422).str.lstrip("0"))/100
    df1['Signo importe cargos'] = df1['Columna_ayuda'].str.slice(start=422, stop=423)
    df1['Total de Importe iva cargos'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=423, stop=436).str.lstrip("0"))/100
    df1['Signo importe iva cargos'] = df1['Columna_ayuda'].str.slice(start=436, stop=437)
    df1['Total de Importe cargos convertido'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=437, stop=450).str.lstrip("0"))/100
    df1['Signo cargos convertido'] = df1['Columna_ayuda'].str.slice(start=450, stop=451)
    df1['Total de Importe iva cargos convertido'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=451, stop=464).str.lstrip("0"))/100
    df1['Signo iva cargos convertido'] = df1['Columna_ayuda'].str.slice(start=464, stop=465)
    df1['Concepto cargo 1'] = df1['Columna_ayuda'].str.slice(start=465, stop=470)
    df1['Importe cargo 1'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=470, stop=483).str.lstrip("0"))/100
    df1['Signo Importe cargo 1'] = df1['Columna_ayuda'].str.slice(start=483, stop=484)
    df1['Importe iva cargo 1'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=484, stop=497).str.lstrip("0"))/100
    df1['Concepto cargo 2'] = df1['Columna_ayuda'].str.slice(start=497, stop=502)
    df1['Importe cargo 2'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=502, stop=515).str.lstrip("0"))/100
    df1['Signo Importe cargo 2'] = df1['Columna_ayuda'].str.slice(start=515, stop=516)
    df1['Importe iva cargo 2'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=516, stop=529).str.lstrip("0"))/100
    df1['Concepto cargo 3'] = df1['Columna_ayuda'].str.slice(start=529, stop=534)
    df1['Importe cargo 3'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=534, stop=547).str.lstrip("0"))/100
    df1['Signo Importe cargo 3'] = df1['Columna_ayuda'].str.slice(start=547, stop=548)
    df1['Importe iva cargo 3'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=548, stop=561).str.lstrip("0"))/100
    df1['Columna adicional 1'] = ""
    df1['Columna adicional 2'] = ""
    df1['Columna adicional 3'] = ""

   #Borro las dos primeras columnas
    df1 = df1.drop(df1.columns[[0, 1]], axis='columns')

    #Reseteo el index
    df1 = df1.set_index('Nro. de Cuenta')
    file_csv = df1 

    return file_csv 

def upload_s3 (file):
    date = datetime.datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")
    filename = 'Liquidaciones Metabase/T7001D/T7001_'+date+'.csv'
    logger.info(f"df_{date}")

    # LO TIRO EN S3
    bucket = 'liquidaciones-conciliaciones' 
    csv_buffer = StringIO()
    file.to_csv(csv_buffer)
    dt = datetime.datetime.now()
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())
    logger.info(f"The dataframe was uploaded with name {filename}")


def upload_rar():
    a = os.listdir('/tmp/')
    yesterday_filter = (datetime.datetime.now()).strftime("%Y%m%d")
    files = [x for x in a if x.find(yesterday_filter) != -1]
    for i in range(len(files)):
        if ('.rar') in files[i]:
            file = files[i]
            path = '/tmp'
            file = path + '/' + file
            print(file)
            nombre = files[i]
            date = datetime.datetime.now().strftime("%Y_%m_%d")
            filename = 'rar/'+yesterday_filter+'/'+nombre
            bucket = 'liquidaciones-conciliaciones' 
            s3_resource = boto3.client('s3')
            s3_resource.upload_file(
                Filename=file,
                Bucket=bucket,
                Key=filename,
            )
            if ('T7001D') in nombre and ('.rar') in nombre:
                bucket = 'liquidaciones-conciliaciones' 
                filename2='GlobalProcessing/'+nombre
                s3_resource = boto3.client('s3')
                s3_resource.upload_file(
                Filename=file,
                Bucket=bucket,
                Key=filename2,
                )
            else:
                continue
        print('Rar Uploaded')

def lambda_handler(event, context):
    files = get_files_from_sftp()
    upload_rar()
    extract_all(files)
    t7001d = get_t7001d_file()
    out_df = clean_txt(t7001d)
    upload_s3(out_df)


