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
    yesterday_filter = (datetime.datetime.now()- datetime.timedelta(hours=4)).strftime("%Y%m%d")
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
        if ('T2001') in f:
            if not os.path.exists(f"/tmp/{f}"):
                logger.warn(f"File {f} does not exists. Skipping")
                continue
            rar = rarfile.RarFile(f"/tmp/{f}")
            rar.extractall('/tmp/')
            os.remove(f"/tmp/{f}")
            logger.info(f"File{f} successfully extracted")


def get_t2001d_file():
    out = [x for x in os.listdir("/tmp/") if x.startswith("T2001D")]
    if len(out) > 1:
        raise Exception(f"I got more than one T2001D's files. {out}")
    if len(out) == 0:
        raise Exception(f"I didn't find any file for T2001D format")
    return out[0]

def clean_txt (file):
    logger.info(f"Processing {file}")
    df1 = pd.read_csv(f'/tmp/{file}',encoding='latin-1',delimiter = '\t')
    df1 = df1[:-1]

    # GENERO NUEVA COLUMNA DE AYUDA
    df1['Columna_ayuda'] = df1.apply(lambda x:x [0:])
    # CREOR COLUMNAS SEGUN EL LAYOUT

    df1['Numero de tarjeta'] = df1['Columna_ayuda'].str.slice(start=13, stop=17)
    df1['Número de cuenta'] = df1['Columna_ayuda'].str.slice(start=32, stop=42)
    df1['Numero de comercio'] = df1['Columna_ayuda'].str.slice(start=50, stop=65).str.replace("'","")
    df1['Nombre fantasía comercio'] = df1['Columna_ayuda'].str.slice(start=65, stop=87).str.replace("'","")
    df1['Código de Autorización'] = df1['Columna_ayuda'].str.slice(start=97, stop=103)
    df1['ICA Adquirente'] = df1['Columna_ayuda'].str.slice(start=103, stop=114)
    df1['Descripción del movimiento'] = df1['Columna_ayuda'].str.slice(start=119, stop=149)

    df1['Fecha y Hora de la operación '] = pd.to_datetime(df1['Columna_ayuda'].str.slice(start=180, stop=194),format='%Y%m%d%H%M%S')
    #df1['Fecha de operación y Hora de la operación '] = df1['Columna_ayuda'].str.slice(start=180, stop=188)
    #df1['Hora de la operación'] = df1['Columna_ayuda'].str.slice(start=188, stop=194)

    df1['Fecha de presentación'] = pd.to_datetime(df1['Columna_ayuda'].str.slice(start=194, stop=202),format='%Y%m%d')
    df1['Importe en moneda del movimiento'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=234, stop=248).str.lstrip("0"))/10000
    df1['Código de moneda original'] = df1['Columna_ayuda'].str.slice(start=248, stop=251)
    df1['Importe en moneda original'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=251, stop=265).str.lstrip("0"))/10000
    df1['Importe interés'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=265, stop=279).str.lstrip("0"))/10000
    df1['Importe del IVA'] =pd.to_numeric( df1['Columna_ayuda'].str.slice(start=279, stop=293).str.lstrip("0"))/10000
    df1['Importe total del movimiento'] =pd.to_numeric( df1['Columna_ayuda'].str.slice(start=293, stop=307).str.lstrip("0"))/10000
    df1['Importe Descuento financ otrog'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=307, stop=321).str.lstrip("0"))/10000
    df1['Importe Descuento financ otrog IVA'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=321, stop=335).str.lstrip("0"))/10000
    df1['Importe de Compensación'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=453, stop=466).str.lstrip("0"))/10000
    df1['Signo importe Compensación'] = df1['Columna_ayuda'].str.slice(start=466, stop=467)
    df1['Importe percep RG4240'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=467, stop=480).str.lstrip("0"))/10000
    df1['Signo percep RG4240'] = df1['Columna_ayuda'].str.slice(start=480, stop=481)
    df1['Moneda de compensación'] = df1['Columna_ayuda'].str.slice(start=495, stop=498)
    df1['Importe autorizacion'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=498, stop=511).str.lstrip("0"))/10000
    df1['Signo importe autorizacion'] = df1['Columna_ayuda'].str.slice(start=511, stop=512)
    df1['Moneda de autorizacion'] = df1['Columna_ayuda'].str.slice(start=512, stop=515)
    df1['Importe autorizacion convertido'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=515, stop=528).str.lstrip("0"))/10000
    df1['Signo importe autorizacion2'] = df1['Columna_ayuda'].str.slice(start=511, stop=512)
    df1['Moneda  autorizacion  convertido'] = df1['Columna_ayuda'].str.slice(start=529, stop=532)
    df1['Importe ipm de6'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=532, stop=545).str.lstrip("0"))/10000
    df1['Signo importe ipm de6'] = df1['Columna_ayuda'].str.slice(start=545, stop=546)
    df1['Moneda  ipm DE51'] = df1['Columna_ayuda'].str.slice(start=546, stop=549)
    df1['Cuenta externa'] = df1['Columna_ayuda'].str.slice(start=569, stop=1069)
    df1['Request_id de la autorizacion'] = df1['Columna_ayuda'].str.slice(start=1069, stop=1119)
    df1['Importe percep Ley 27.541'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=1119, stop=1132).str.lstrip("0"))/10000
    df1['Signo percep Ley 27.541'] = df1['Columna_ayuda'].str.slice(start=1132, stop=1133)
    df1['IVA Servicios Digitales - Importe'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=1133, stop=1146).str.lstrip("0"))/10000
    df1['IVA Servicios Digitales - Signo'] = df1['Columna_ayuda'].str.slice(start=1146, stop=1147)
    df1['IIBB Servicios Digitales - Importe'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=1147, stop=1160).str.lstrip("0"))/10000
    df1['IIBB Servicios Digitales - Signo'] = df1['Columna_ayuda'].str.slice(start=1160, stop=1161)
    df1['IIBB Servicios Digitales - Código de provincia'] = df1['Columna_ayuda'].str.slice(start=1161, stop=1163)
    df1['RG. 4815 - Importe'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=1183, stop=1196).str.lstrip("0"))/10000
    df1['RG. 4815 - Signo'] = df1['Columna_ayuda'].str.slice(start=1196, stop=1197)
    df1['Tipo Registro'] = df1['Columna_ayuda'].str.slice(start=0, stop=1)
    df1['Marca'] = df1['Columna_ayuda'].str.slice(start=17, stop=22)
    df1['Entidad emisora'] = df1['Columna_ayuda'].str.slice(start=22, stop=27)
    df1['Sucursal'] = df1['Columna_ayuda'].str.slice(start=27, stop=32)
    df1['Tipo de socio'] = df1['Columna_ayuda'].str.slice(start=42, stop=44)
    df1['Grupo de cuenta corriente'] = df1['Columna_ayuda'].str.slice(start=44, stop=47)
    df1['Tipo de transacción'] = df1['Columna_ayuda'].str.slice(start=47, stop=50)
    df1['Código Postal Comercio'] = df1['Columna_ayuda'].str.slice(start=87, stop=97)
    df1['Codigo de movimiento'] = df1['Columna_ayuda'].str.slice(start=114, stop=119)
    df1['Tipo de Plan'] = df1['Columna_ayuda'].str.slice(start=172, stop=174)
    df1['Plan de cuotas'] = df1['Columna_ayuda'].str.slice(start=174, stop=177)
    df1['Número de cuota vigente'] = df1['Columna_ayuda'].str.slice(start=177, stop=180)
    df1['Código de moneda'] = df1['Columna_ayuda'].str.slice(start=231, stop=234)
    df1['DEBCRED'] = df1['Columna_ayuda'].str.slice(start=391, stop=392)
    df1['Tipo amortización'] = df1['Columna_ayuda'].str.slice(start=392, stop=393)
    df1['Tipo Tarjeta'] = df1['Columna_ayuda'].str.slice(start=393, stop=394)
    df1['TNA'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=394, stop=403).str.lstrip("0"))/10000
    df1['TEA'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=403, stop=412).str.lstrip("0"))/10000
    df1['Tasa de Intercambio'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=412, stop=421).str.lstrip("0"))/10000
    df1['Arancel emisor'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=421, stop=434).str.lstrip("0"))/10000
    df1['Signo Iva Arancel'] = df1['Columna_ayuda'].str.slice(start=434, stop=435)
    df1['IVA arancel emisor'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=435, stop=448).str.lstrip("0"))/10000
    df1['Signo Iva Arancel2'] = df1['Columna_ayuda'].str.slice(start=434, stop=435)
    df1['Motivo mensaje'] = df1['Columna_ayuda'].str.slice(start=449, stop=453)
    df1['Tipo producto'] = df1['Columna_ayuda'].str.slice(start=481, stop=484)
    df1['Fecha de cierre de comercios'] = df1['Columna_ayuda'].str.slice(start=484, stop=492)
    df1['Plan'] = df1['Columna_ayuda'].str.slice(start=492, stop=495)
    df1['Estado'] = df1['Columna_ayuda'].str.slice(start=1163, stop=1183)

    #el filler habría que revisarlo, está raro, pero como no lo usamos, pasa el MVP
    df1['filler'] = df1['Columna_ayuda'].str.slice(start=1196, stop=1500)

    df1['Autoid'] = df1['Columna_ayuda'].str.slice(start=549, stop=559)
    df1['Autocodi'] = df1['Columna_ayuda'].str.slice(start=559, stop=569)
    df1['Comprobante'] = df1['Columna_ayuda'].str.slice(start=149, stop=172)
    df1['Importe de la cuota'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=335, stop=349).str.lstrip("0"))/10000
    df1['Importe interés de la cuota'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=349, stop=363).str.lstrip("0"))/10000
    df1['Import del IVA de la cuota'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=363, stop=377).str.lstrip("0"))/10000
    df1['Importe total de la cuota'] = pd.to_numeric(df1['Columna_ayuda'].str.slice(start=377, stop=391).str.lstrip("0"))/10000
    df1['Fecha de cierre de cuenta corriente'] = pd.to_datetime(df1['Columna_ayuda'].str.slice(start=202, stop=210),format='%Y%m%d')
    df1['Fecha de clearing'] = pd.to_datetime(df1['Columna_ayuda'].str.slice(start=210, stop=218),format='%Y%m%d')
    df1['Fecha de diferimiento'] = pd.to_datetime(df1['Columna_ayuda'].str.slice(start=218, stop=226),format='%Y%m%d')
    df1['MCC'] = df1['Columna_ayuda'].str.slice(start=226, stop=231)
    df1['Columna adicional 1'] = ""
    df1['Columna adicional 2'] = ""
    df1['Columna adicional 3'] = ""

    # BORRO LAS DOS PRIMERAS COLUMNAS
    df1 = df1.drop(df1.columns[[0, 1]], axis='columns')

    # RESETEO EL INDEX
    df1 = df1.set_index('Numero de tarjeta')
    file_csv = df1 

    return file_csv 

def upload_s3 (file):
    # CREO EL NOMBRE DEL FILE 
    date = datetime.datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")
    filename = 'Liquidaciones Metabase/T2001D/T2001_'+date+'.csv'
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
            if ('T2001D') in nombre and ('.rar') in nombre:
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
        
def upload_backfill():
    a = os.listdir('/tmp/')
    print(datetime.datetime.now()- datetime.timedelta(hours=4))
    yesterday_filter = (datetime.datetime.now()- datetime.timedelta(hours=4)).strftime("%Y%m%d")
    files = [x for x in a if x.find(yesterday_filter) != -1]
    for i in range(len(files)):
        file = files[i]
        path = '/tmp'
        file = path + '/' + file
        print(file)
        nombre = files[i]
        date = (datetime.datetime.now()- datetime.timedelta(hours=4)).strftime("%Y_%m_%d")
        filename = 'rar/'+yesterday_filter+'/'+nombre
        bucket = 'liquidaciones-conciliaciones' 
        s3_resource = boto3.client('s3')
        s3_resource.upload_file(
            Filename=file,
            Bucket=bucket,
            Key=filename,
        )
        print('Rar Uploaded')

def lambda_handler(event, context):
    event_type = event['RUN']
    if event_type == 'BACKFILL':
        files = get_files_from_sftp()
        upload_backfill()
    else:
        files = get_files_from_sftp()
        upload_rar()
        extract_all(files)
        t2001d = get_t2001d_file()
        out_df = clean_txt(t2001d)
        upload_s3(out_df)
