from airflow import DAG, settings, secrets
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.sql import SQLCheckOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
    dag_id = "clean_commerces_name",
    start_date = datetime(2022, 10, 17),
    schedule_interval = '0 6 * * 1', 
    tags = ['tabla', 'commerces']
    ) as dag:

    # Staging table
    create_staging_table = RedshiftSQLOperator(
        task_id = 'create_staging_table',
        sql = """
                CREATE TABLE lemoncash_data.commerces_staging_table(
                    "code" varchar,
                    "name" varchar,
                    "category" varchar,
                    "merchant_category_code" varchar
                ); """)
    
    # Limpieza comercios
    insert_clean_commerces = RedshiftSQLOperator(
        task_id = "insert_clean_commerces",
        sql = """
                INSERT INTO lemoncash_data.commerces_staging_table (code, name, category, merchant_category_code)
                SELECT
                        code,
                        comercio as name,
                        category,
                        merchant_category_code
                FROM (
                    with new_commerces as (
                    select * from lemoncash_ar.lemoncardpaymentcommerces
                    where code not in (select code from lemoncash_data.commerces)
                ),

                clean_names as (
                    select
                        distinct code,
                        case
                            when charindex('pedidos ya', lower(name)) > 0 or charindex('pedidosya', lower(name)) > 0 then 'PEDIDOS YA'
                            when charindex('mcdonald', lower(name)) > 0 or charindex('mc donals', lower(name)) > 0
                                or charindex('macdonald s', lower(name)) > 0 or charindex('mac donalds', lower(name)) > 0
                                or charindex('mc ', lower(name)) > 0 or charindex('arcos dorados', lower(name)) > 0
                                or charindex('mc donald', lower(name)) > 0 then 'MC DONALDS'
                            when charindex('burger king', lower(name)) > 0 or (charindex('bk ', lower(name)) > 0 and charindex('facebk', lower(name)) <= 0)
                                or lower(name) = 'bk valentin alsina' or charindex('burgeer king', lower(name)) > 0
                                or charindex('burgerking', lower(name)) > 0 then 'BURGER KING'
                            when charindex('mostaza', lower(name)) > 0 then 'MOSTAZA'
                            when charindex('wendys', lower(name)) > 0 then 'WENDYS'
                            when charindex('big pon', lower(name)) > 0 then 'BIG PONS'
                            when charindex('baum', lower(name)) > 0 then 'BAUM'
                            when charindex('birra bar', lower(name)) > 0 then 'LA BIRRA BAR'
                            when charindex('bellagamba', lower(name)) > 0 then 'BELLAGAMBA'
                            when charindex('williamsbur', lower(name)) > 0 then 'WILLIAMSBURG'
                            when charindex('dean dennys', lower(name)) > 0 then 'DEAN AND DENNYS'
                            when charindex('kfc', lower(name)) > 0 then 'KFC'
                            when charindex('mercadopago*mercadolib', lower(name)) > 0 then 'MERCADOLIBRE'
                            when charindex('pagocredit', lower(name)) > 0 then 'PAGOCRÉDITO'
                            when charindex('jumbo', lower(name)) > 0 then 'JUMBO'
                            when charindex('disco', lower(name)) > 0 then 'DISCO'
                            when charindex('3sm', lower(name)) > 0 then '3M SUPERMERCADO'
                            when charindex('vea ', lower(name)) > 0 or charindex('veadigital', lower(name)) > 0 then 'VEA'
                            when charindex('carrefour', lower(name)) > 0 or charindex('express', lower(name)) > 0 then 'CARREFOUR'
                            when charindex('coto', lower(name)) > 0 then 'COTO'
                            when charindex('superdia', lower(name)) > 0
                                or charindex('diaonline', lower(name)) > 0
                                or charindex('dia tienda', lower(name)) > 0 then 'SUPERMERCADO DIA'
                            when charindex('walmart', lower(name)) > 0 then 'WALMART'
                            when charindex('laanonima', lower(name)) > 0 or charindex('la anonima', lower(name)) > 0 then 'LA ANÓNIMA'
                            when charindex('vital', lower(name)) > 0 then 'VITAL'
                            when charindex('diarco', lower(name)) > 0 then 'DIARCO'
                            when charindex('makro', lower(name)) > 0 then 'MAKRO'
                            when charindex('el puente', lower(name)) > 0 or charindex('elpuente', lower(name)) > 0 then 'EL PUENTE LACTEOS'
                            when charindex('chango', lower(name)) > 0 or charindex('hiperchang', lower(name)) > 0 then 'CHANGO MAS'
                            when charindex('super luna', lower(name)) > 0 then 'SUPER LUNA'
                            when charindex('superpampero', lower(name)) > 0 then 'SUPER PAMPERO'
                            when charindex('ekomarket', lower(name)) > 0 then 'EKO MARKET'
                            when charindex('fresh market', lower(name)) > 0 then 'FRESH MARKET'
                            when charindex('delicias d', lower(name)) > 0 then 'LAS DELICIAS'
                            when charindex('maxiconsumo', lower(name)) > 0 then 'MAXICONSUMO'
                            when charindex('sodimac', lower(name)) > 0 then 'SODIMAC'
                            when charindex('easy', lower(name)) > 0 then 'EASY'
                            when charindex('fravega', lower(name)) > 0 then 'FRAVEGA'
                            when charindex('megatone', lower(name)) > 0 then 'MEGATONE'
                            when charindex('servicentro', lower(name)) > 0 then 'SERVICENTRO'
                            when charindex('farmacity', lower(name)) > 0 or charindex('farmcity', lower(name)) > 0 then 'FARMACITY'
                            when charindex('natura', lower(name)) > 0 then 'NATURA'
                            when charindex('avon', lower(name)) > 0 then 'AVON'
                            when charindex('cuspide', lower(name)) > 0 then 'CUSPIDE'
                            when charindex('open25', lower(name)) > 0 or charindex('open 25', lower(name)) > 0 then 'OPEN 25'
                            when charindex('open 24', lower(name)) > 0 then 'OPEN 24'
                            when charindex('cafe martinez', lower(name)) > 0 or charindex('cafemartin', lower(name)) > 0 then 'CAFÉ MARTÍNEZ'
                            when charindex('havanna', lower(name)) > 0 or charindex('havana', lower(name)) > 0 then 'HAVANNA'
                            when charindex('bonafide', lower(name)) > 0 then 'BONAFIDE'
                            when charindex('panera rosa', lower(name)) > 0 then 'LA PANERA ROSA'
                            when charindex('locos por el asado', lower(name)) > 0 then 'LOCOS POR EL ASADO'
                            when charindex('starbuck', lower(name)) > 0
                                or charindex('starbuks', lower(name)) > 0
                                or charindex('starburcks', lower(name)) > 0
                                or charindex('sbx', lower(name)) > 0
                                or charindex('sbux', lower(name)) > 0
                                or charindex('stbx', lower(name)) > 0 then 'STARBUCKS'
                            when charindex('grido', lower(name)) > 0 then 'GRIDO'
                            when charindex('freddo', lower(name)) > 0 then 'FREDDO'
                            when charindex('chungo', lower(name)) > 0 then 'CHUNGO'
                            when charindex('lucciano', lower(name)) > 0 then 'LUCCIANOS'
                            when charindex('nicolo', lower(name)) > 0 then 'NICOLO'
                            when charindex('rapanui', lower(name)) > 0 or charindex('rapa nui', lower(name)) > 0 then 'RAPANUI'
                            when charindex('antares', lower(name)) > 0 then 'ANTARES'
                            when charindex('rabieta', lower(name)) > 0 then 'RABIETA'
                            when charindex('patagonia', lower(name)) > 0 then 'PATAGONIA BAR'
                            when charindex('cruza', lower(name)) > 0 then 'CRUZA'
                            when charindex('temple', lower(name)) > 0 then 'TEMPLE BAR'
                            when charindex('banana', lower(name)) > 0 or charindex('bnn', lower(name)) > 0 then 'BANANA'
                            when charindex('tomasso', lower(name)) > 0 then 'TOMASSO'
                            when charindex('brozziano', lower(name)) > 0 then 'BROZZIANO'
                            when charindex('costumbres', lower(name)) > 0 then 'COSTUMBRES ARGENTINAS'
                            when charindex('kentucky', lower(name)) > 0 then 'KENTUCKY PIZZA'
                            when charindex('johnny b good', lower(name)) > 0 then 'JOHNNY B GOOD'
                            when charindex('atalaya', lower(name)) > 0 then 'PARADOR ATALAYA'
                            when charindex('kansas', lower(name)) > 0 then 'KANSAS'
                            when charindex('ypf', lower(name)) > 0 or charindex('full 24', lower(name)) > 0 or charindex('y p f', lower(name)) > 0 then 'YPF'
                            when charindex('shell', lower(name)) > 0 then 'SHELL'
                            when charindex('pumaenergy', lower(name)) > 0 or charindex('serv puma', lower(name)) > 0 then 'PUMA ENERGY'
                            when charindex('axion', lower(name)) > 0 then 'AXION'
                            when charindex('petrob', lower(name)) > 0 then 'PETROBRAS'
                            when charindex('esso', lower(name)) > 0 then 'ESSO'
                            when charindex('uber', lower(name)) > 0 then 'UBER'
                            when charindex('cabify', lower(name)) > 0 then 'CABIFY'
                            when charindex('lyft', lower(name)) > 0 then 'LYFT'
                            when charindex('didi', lower(name)) > 0 then 'DIDI'
                            when charindex('plataforma10', lower(name)) > 0 then 'PLATAFORMA10'
                            when charindex('flechabus', lower(name)) > 0 then 'FLECHABUS'
                            when charindex('pasajesweb', lower(name)) > 0 then 'PASAJES WEB'
                            when charindex('via bariloche', lower(name)) > 0 then 'VIA BARILOCHE'
                            when charindex('chevallier', lower(name)) > 0 then 'CHEVALLIER'
                            when charindex('andesmar', lower(name)) > 0 then 'ANDES MAR'
                            when charindex('ebay', lower(name)) > 0 then 'EBAY'
                            when charindex('steam', lower(name)) > 0 then 'STEAM'
                            when charindex('supercell', lower(name)) > 0 then 'SUPERCELL'
                            when charindex('fortnite', lower(name)) > 0 or charindex('epic games', lower(name)) > 0 then 'EPIC GAMES'
                            when charindex('playstation', lower(name)) > 0 then 'PLAYSTATION NETWORK'
                            when charindex('roblox', lower(name)) > 0 then 'ROBLOX'
                            when charindex('paramount', lower(name)) > 0 then 'PARAMOUNT PLUS'
                            when charindex('xbox', lower(name)) > 0 then 'XBOX'
                            when charindex('riot*', lower(name)) > 0 then 'RIOT GAMES'
                            when charindex('origin.com', lower(name)) > 0 or charindex('electronic arts', lower(name)) > 0 then 'ELECTRONIC ARTS'
                            when charindex('blizzard', lower(name)) > 0 then 'BLIZZARD ENTERTAINMENT'
                            when charindex('apple', lower(name)) > 0 then 'APPLE.COM'
                            when charindex('amzn', lower(name)) > 0 then 'AMAZON'
                            when charindex('google', lower(name)) > 0 then 'GOOGLE'
                            when charindex('facebk', lower(name)) > 0 then 'FACEBOOK'
                            when charindex('samsung', lower(name)) > 0 then 'SAMSUNG'
                            when charindex('envato', lower(name)) > 0 then 'ENVATO'
                            when charindex('microsoft', lower(name)) > 0 then 'MICROSOFT STORE'
                            when charindex('prezi', lower(name)) > 0 then 'PREZI'
                            when charindex('rappi', lower(name)) > 0 then 'RAPPI'
                            when charindex('tada', lower(name)) > 0 then 'TADA'
                            when charindex('appbar', lower(name)) > 0 then 'APPBAR'
                            when charindex('correoargentino', lower(name)) > 0 then 'CORREO ARGENTINO'
                            when charindex('andreani', lower(name)) > 0 then 'ANDREANI'
                            when charindex('movistar', lower(name)) > 0 then 'MOVISTAR'
                            when charindex('huawei', lower(name)) > 0 then 'HUAWEI'
                            when charindex('personal', lower(name)) > 0 then 'PERSONAL'
                            when charindex('claro', lower(name)) > 0 then 'CLARO'
                            when charindex('tuenti', lower(name)) > 0 or charindex('recargatuent', lower(name)) > 0 then 'TUENTI'
                            when charindex('telecom', lower(name)) > 0 then 'TELECOM'
                            when charindex('telecentro', lower(name)) > 0 then 'TELECENTRO'
                            when charindex('iplan', lower(name)) > 0 then 'IPLAN'
                            when charindex('telefonica', lower(name)) > 0 then 'TELEFONICA'
                            when charindex('cablevisin', lower(name)) > 0 then 'CABLEVISION'
                            when charindex('flow', lower(name)) > 0 then 'PERSONAL FLOW'
                            when charindex('directv', lower(name)) > 0 or charindex('direc tv', lower(name)) > 0 then 'DIRECTV'
                            when charindex('arba', lower(name)) > 0 then 'ARBA'
                            when charindex('aguasbonae', lower(name)) > 0 then 'ABSA'
                            when charindex('aysa', lower(name)) > 0 then 'AYSA'
                            when charindex('edea', lower(name)) > 0 then 'EDEA'
                            when charindex('edenor', lower(name)) > 0 then 'EDENOR'
                            when charindex('edesur', lower(name)) > 0 then 'EDESUR'
                            when charindex('eden sa', lower(name)) > 0 then 'EDEN SA'
                            when charindex('enersa', lower(name)) > 0 then 'ENERSA'
                            when charindex('metrogas', lower(name)) > 0 then 'METROGAS'
                            when charindex('naturgy', lower(name)) > 0 then 'NATURGY'
                            when charindex('camuzzi', lower(name)) > 0 then 'CAMUZZI GAS'
                            when charindex('petrogas', lower(name)) > 0 then 'PETROGAS'
                            when charindex('ecogas', lower(name)) > 0 then 'ECO GAS'
                            when charindex('udemy', lower(name)) > 0 or charindex('udemm', lower(name)) > 0 then 'UDEMY'
                            when charindex('domestika', lower(name)) > 0 then 'DOMESTIKA'
                            when charindex('osde', lower(name)) > 0 then 'OSDE'
                            when charindex('medife', lower(name)) > 0 then 'MEDIFE'
                            when charindex('ospe', lower(name)) > 0 then 'OSPE'
                            when charindex('swiss medical', lower(name)) > 0 then 'SWISS MEDICAL'
                            when charindex('abasto', lower(name)) > 0 then 'ABASTO'
                            when charindex('cinepolis', lower(name)) > 0 then 'CINEPOLIS'
                            when charindex('cinemark', lower(name)) > 0 then 'CINEMARK'
                            when charindex('hoyts', lower(name)) > 0 then 'HOYTS'
                            when charindex('showcase', lower(name)) > 0 then 'SHOWCASE'
                            when charindex('showcenter', lower(name)) > 0 then 'SHOWCENTER'
                            when charindex('bresh', lower(name)) > 0 then 'BRESH'
                            when charindex('ticketek', lower(name)) > 0 then 'TICKETEK'
                            when charindex('tiketpass', lower(name)) > 0 then 'TICKETPASS'
                            when charindex('ticketportal', lower(name)) > 0 then 'TICKET PORTAL'
                            when charindex('ticketing', lower(name)) > 0 then 'TICKETING'
                            when charindex('plateanet', lower(name)) > 0 then 'PLATEANET'
                            when charindex('all access', lower(name)) > 0 then 'ALL ACCESS'
                            when charindex('passline', lower(name)) > 0 then 'PASSLINE'
                            when charindex('livepass', lower(name)) > 0 then 'LIVEPASS'
                            when charindex('tuentrada', lower(name)) > 0 or charindex('tu entrada', lower(name)) > 0 then 'TUENTRADA'
                            when charindex('entradauno', lower(name)) > 0 then 'ENTRADAUNO'
                            when charindex('boleteria', lower(name)) > 0 then 'MIBOLETERIA'
                            when charindex('spotify', lower(name)) > 0 then 'SPOTIFY'
                            when charindex('netflix', lower(name)) > 0 then 'NETFLIX'
                            when charindex('twitch', lower(name)) > 0 then 'TWITCH.TV'
                            when charindex('crulcpchyroll', lower(name)) > 0 then 'CRUlcpcHYROLL'
                            when charindex('youtube', lower(name)) > 0 then 'YOUTUBE PREMIUM'
                            when charindex('disney plus', lower(name)) > 0 then 'DISNEY PLUS'
                            when charindex('prime video', lower(name)) > 0 or charindex('amazon prime', lower(name)) > 0 then 'PRIME VIDEO'
                            when charindex('hbo', lower(name)) > 0 then 'HBO'
                            when charindex('star plus', lower(name)) > 0 then 'STAR PLUS'
                            when charindex('pagos360', lower(name)) > 0 then 'PAGOS 360'
                            when charindex('jetsmart', lower(name)) > 0 then 'JETSMART'
                            when charindex('ecobici', lower(name)) > 0 then 'ECOBICI'
                            when charindex('tinder', lower(name)) > 0 then 'TINDER'
                            when charindex('badoo', lower(name)) > 0 then 'BADOO'
                            when charindex('onlyfans', lower(name)) > 0 then 'ONLY FANS'
                            when charindex('booking', lower(name)) > 0 then 'BOOKING.COM'
                            when charindex('despegar', lower(name)) > 0 then 'DESPEGAR'
                            when charindex('flybondi', lower(name)) > 0 then 'FLYBONDI'
                            when charindex('aerolineas', lower(name)) > 0 then 'AEROLÍNEAS ARGENTINAS'
                            when charindex('amx', lower(name)) > 0 then 'AMX ARGENTINA'
                            when charindex('deheza', lower(name)) > 0 then 'DEHEZA'
                            when charindex('carp', lower(name)) > 0 or charindex('riverplate', lower(name)) > 0 then 'RIVER PLATE'
                            when charindex('bocajuniors', lower(name)) > 0 then 'BOCA JUNIORS'
                            when charindex('puma', lower(name)) > 0 then 'PUMA'
                            when charindex('nike', lower(name)) > 0 then 'NIKE'
                            when charindex('adidas', lower(name)) > 0 then 'ADIDAS'
                            when charindex('montagne', lower(name)) > 0 then 'MONTAGNE'
                            when charindex('dextershop', lower(name)) > 0 then 'DEXTER'
                            when charindex('zara', lower(name)) > 0 then 'ZARA'
                            when charindex('herbalife', lower(name)) > 0 then 'HERBALIFE'
                            when charindex('sube', lower(name)) > 0 then 'SUBE'
                            when charindex('*aca', lower(name)) > 0 or charindex('aca ', lower(name)) > 0 then 'ACA'
                            when charindex('paypal', lower(name)) > 0 then 'PAYPAL'
                            when charindex('smiles', lower(name)) > 0 then 'SMILES'
                            when charindex('amway', lower(name)) > 0 then 'AMWAY'
                            when charindex('bidcom', lower(name)) > 0 then 'BID.COM'
                            when charindex('ontap', lower(name)) > 0 then 'ONTAP'
                            when charindex('renaper', lower(name)) > 0 then 'RENAPER'
                            when charindex('mercadopago*', lower(name)) > 0
                                or charindex('merpago', lower(name)) > 0
                                or charindex('mercpago', lower(name)) > 0 
                                or charindex('mpago', lower(name)) > 0 then 'MERCADOPAGO'
                            when charindex('mercadolibre', lower(name)) > 0 then 'MERCADOLIBRE'
                            when charindex('est de serv', lower(name)) > 0
                                or charindex('estacion de serv', lower(name)) > 0
                                or charindex('est serv', lower(name)) > 0
                                or charindex('estac de serv', lower(name)) > 0
                                or charindex('combustibles y servici', lower(name)) > 0
                                or charindex('estacion de servicio', lower(name)) > 0 then 'OTROS - ESTACIONES DE SERVICIO'
                            when charindex('duty free shop', lower(name)) > 0 then 'DUTY FREE SHOP'
                            when charindex('buenosaires.gov', lower(name)) > 0 or charindex('buenosaires.gob', lower(name)) > 0 then 'GCBA'
                            when charindex('afip', lower(name)) > 0 then 'AFIP'
                        else upper(name)
                        end as comercio,
                        case
                            when comercio in ('PEDIDOS YA', 'MC DONALDS', 'BURGER KING', 'MOSTAZA', 'WENDYS', 'BIG PONS', 'BAUM', 'LA BIRRA BAR', 'BELLAGAMBA','WILLIAMSBURG','DEAN AND DENNYS','KFC', 'LAS DELICIAS','RAPPI','TADA','APPBAR', 'ONTAP','CAFÉ MARTÍNEZ','HAVANNA','BONAFIDE','LA PANERA ROSA','LOCOS POR EL ASADO','STARBUCKS','GRIDO','FREDDO','CHUNGO','LUCCIANOS','NICOLO','RAPANUI','ANTARES', 'RABIETA','PATAGONIA BAR', 'TEMPLE BAR','TOMASSO','BROZZIANO','COSTUMBRES ARGENTINAS','KENTUCKY PIZZA','JOHNNY B GOOD','PARADOR ATALAYA','KANSAS', 'BETOS') then 'FOOD'
                            when comercio in ('PAGOCRÉDITO', 'PAGOS 360','CORREO ARGENTINO','ANDREANI','AMX ARGENTINA','DEHEZA') then 'OTHER SERVICES'
                            when comercio in ('JUMBO','DISCO','3M SUPERMERCADO','VEA','CARREFOUR','COTO','SUPERMERCADO DIA','WALMART','LA ANÓNIMA', 'VITAL','DIARCO','MAKRO','EL PUENTE LACTEOS', 'CHANGO MAS','SUPER LUNA','SUPER PAMPERO','EKO MARKET','FRESH MARKET') then 'SUPERMARKETS'
                            when comercio in ('OPEN 25','OPEN 24','CUSPIDE','MAXICONSUMO','SERVICENTRO', 'DUTY FREE SHOP', 'ALIBABA') then 'STORES'
                            when comercio in ('SODIMAC','EASY','FRAVEGA','MEGATONE') then 'HOME SERVICES & STORES'
                            when comercio in ('YPF','SHELL','PUMA ENERGY','AXION','PETROBRAS','ESSO','ACA') then 'CAR'
                            when comercio in ('FARMACITY', 'NATURA','AVON', 'OSDE','MEDIFE','OSPE','SWISS MEDICAL','HERBALIFE') then 'HEALTHCARE'
                            when comercio in ('UBER','CABIFY','LYFT','DIDI','PLATAFORMA10','FLECHABUS','PASAJES WEB','VIA BARILOCHE','CHEVALLIER', 'ANDES MAR','SMILES','SUBE', 'JETSMART','ECOBICI','BOOKING.COM','DESPEGAR''FLYBONDI','AEROLÍNEAS ARGENTINAS') then 'TRAVEL'
                            when comercio in ('EBAY','STEAM','SUPERCELL','EPIC GAMES','PLAYSTATION NETWORK','ROBLOX','PARAMOUNT PLUS','XBOX', 'RIOT GAMES','ELECTRONIC ARTS','BLIZZARD ENTERTAINMENT','APPLE.COM','AMAZON','GOOGLE','FACEBOOK', 'ENVATO','MICROSOFT STORE', 'SPOTIFY','NETFLIX','TWITCH.TV','CRUlcpcHYROLL','YOUTUBE PREMIUM','DISNEY PLUS','PRIME VIDEO','HBO','STAR PLUS','TINDER','BADOO','ONLY FANS','PAYPAL','AMWAY') then 'DIGITAL GOODS'
                            when comercio in ('SAMSUNG','HUAWEI') then 'TECHNOLOGY'
                            when comercio in ('PUMA','NIKE','ADIDAS','MONTAGNE','DEXTER','ZARA') then 'CLOTHES'
                            when comercio in ('PERSONAL','CLARO','TUENTI','TELECOM','TELECENTRO', 'IPLAN','TELEFONICA','CABLEVISION','PERSONAL FLOW','DIRECTV','MOVISTAR') then 'MEDIA'
                            when comercio in ('UDEMY','DOMESTIKA','PREZI') then 'EDUCATION'
                            when comercio in ('ABASTO','CINEPOLIS','CINEMARK','HOYTS','SHOWCASE','SHOWCENTER','BRESH','TICKETEK','TICKETPASS', 'TICKET PORTAL', 'TICKETING','PLATEANET','ALL ACCESS','PASSLINE','LIVEPASS','TUENTRADA','ENTRADAUNO','MIBOLETERIA', 'RIVER PLATE','BOCA JUNIORS','BID.COM','BANANA','CRUZA') then 'ENTERTAINMENT'
                            when comercio in ('RENAPER','GCBA','AFIP','ARBA','ABSA','AYSA','EDEA','EDENOR','EDESUR','EDEN SA','ENERSA','METROGAS', 'NATURGY','CAMUZZI GAS','PETROGAS','ECO GAS') then 'PUBLIC SERVICES & TAXES'
                            when comercio in ('MERCADOPAGO', 'MERCADOLIBRE') then 'MP & MELI'
                            when merchant_category_code in (5013, 5021, 5039, 5044, 5046, 5051, 5065, 5072, 5074, 5085, 5094, 5099, 5131, 5137,  5139,  5169, 5172, 5192, 5193, 5198, 5199, 2791, 2842, 7375, 7379, 7829, 8734) then 'B2B'
                            when merchant_category_code in (5812, 5814, 5422, 5441, 5451, 5462, 5499, 5811, 5812, 5813, 5814, 5921) then 'FOOD'
                            when merchant_category_code in (5611, 5621, 5631, 5641, 5651, 5655, 5661, 5681, 5691, 5697, 5698, 5699, 5931, 5941, 7251, 7631, 7699) then 'CLOTHES'
                            when merchant_category_code = 5411 then 'SUPERMARKETS'
                            when merchant_category_code in (5111, 5309, 5310, 5311, 5331, 5399, 5733, 5735, 5932, 5933, 5937, 5942, 5943, 5944, 5945, 5947, 5948, 5949, 5950, 5964, 5965, 5970, 5971, 5972, 5973, 5977, 5978, 5992, 5993, 5994, 5995, 5996, 5997, 5998, 5999, 7210, 7211, 7216, 7217, 7221, 7230, 7278, 7296, 7531, 7841, 7991, 9751, 9752, 9753, 4225) then 'STORES'
                            when merchant_category_code in (4131, 4511, 3381, 3700, 3604, 3000, 3001, 3002, 3003, 3004, 3005, 3006, 3007, 3008, 3009, 3010, 3011, 3012, 3013, 3014, 3015, 3016, 3017, 3018, 3020, 3021, 3022, 3023, 3024, 3025, 3026, 3027, 3028, 3029, 3031, 3033, 3038, 3044, 3045, 3048, 3049, 3051, 3059, 3060, 3063, 3064, 3065, 3068, 3069, 3070, 3071, 3073, 3075, 3077, 3078, 3079, 3082, 3083, 3084, 3085, 3087, 3090, 3094, 3096, 3098, 3100, 3125, 3129, 3130, 3131, 3132, 3135, 3136, 3148, 3151, 3159, 3161, 3171, 3172, 3174, 3175, 3177, 3178, 3180, 3181, 3183, 3190, 3191, 3193, 3195, 3196, 3197, 3198, 3200, 3204, 3206, 3211, 3222, 3226, 3228, 3236, 3239, 3243, 3246, 3247, 3248, 3252, 3256, 3260, 3261, 3266, 3267, 3268, 3275, 3276, 3277, 3279, 3280, 3282, 3292, 3294, 3295, 3296, 3297, 3298, 3361, 4215, 4511, 4582, 5599, 3263, 3353, 3355, 3359, 3360, 3364, 3366, 3374, 3380, 3381, 3386, 3387, 3388, 3391, 3393, 3395, 3398, 3400, 3409, 3421, 3423, 3425, 3427, 3428, 3429, 3430, 3431, 3432, 3434, 3435, 3436, 3438, 3439, 3441, 3030, 3032, 3034, 3035, 3036, 3037, 3039, 3040, 3041, 3042, 3043, 3046, 3047, 3050, 3052, 3053, 3054, 3055, 3056, 3057, 3058, 3061, 3062, 3066, 3072, 3076, 3089, 3097, 3099, 3102, 3103, 3105, 3106, 3111, 3112, 3117, 3127, 3141, 3144, 3146, 3156, 3164, 3167, 3182, 3184, 3185, 3186, 3187, 3188, 3199, 3207, 3212, 3213, 3217, 3219, 3220, 3221, 3223, 3229, 3231, 3240, 3241, 3242, 3245, 3285, 3286, 3287, 3291, 3293, 3299, 4722, 4011, 4111, 4112, 4121, 4131, 4214, 4411, 4457, 4468, 4789, 4829) then 'TRAVEL'
                            when merchant_category_code in (5561, 5571, 5592, 5598, 5551, 5940) then 'VEHICLES STORES'
                            when merchant_category_code in (780, 1520, 1711, 1731, 1740, 1750, 1761, 1771, 7641, 6513, 5712, 5713, 5714, 5718, 5719, 5722, 5200, 5211, 5231, 5261) then 'HOME SERVICES & STORES'
                            when merchant_category_code in (742, 763, 1799, 2741, 4723, 5935, 7511, 9950, 3352, 4761, 5960, 5961, 5962, 5963, 5966, 5967, 5968, 5969, 6010, 6011, 6012, 6050, 6051, 6211, 6300, 6381, 6399, 6529, 6530, 6531, 6532, 6533, 6534, 6535, 6536, 6537, 6538, 6540, 7261, 7273, 7276, 7277, 7299, 7311, 7321, 7332, 7333, 7338, 7339, 7342, 7349, 7361, 7372, 7392, 7393, 7394, 7395, 7399, 7549, 7692, 7778, 8111, 8911, 8931, 8999) then 'OTHER SERVICES'
                            when merchant_category_code in (4784, 4900, 9222, 9223, 9311, 9399, 9401, 9402, 9405, 9700, 9701, 9702, 9211) then 'PUBLIC SERVICES & TAXES'
                            when merchant_category_code in (4119, 7298, 8099, 8071, 8011, 4199, 5047, 5122, 8021, 8031, 8041, 8042, 8043, 8049, 8050, 8062, 5912, 5975, 5976, 7941, 7297) then 'HEALTHCARE'
                            when merchant_category_code in (4812, 4813, 4814, 4815, 4816, 4821, 4899) then 'MEDIA'
                            when merchant_category_code in (3676, 3710, 3716, 3744, 3787, 3821, 3829, 3501, 3502, 3503, 3504, 3505, 3506, 3507, 3508, 3509, 3510, 3511, 3512, 3513, 3514, 3515, 3516, 3517, 3518, 3519, 3520, 3521, 3522, 3523, 3524, 3525, 3526, 3527, 3528, 3529, 3530, 3531, 3532, 3533, 3534, 3535, 3536, 3537, 3538, 3539, 3540, 3541, 3542, 3543, 3544, 3545, 3546, 3548, 3549, 3550, 3551, 3552, 3553, 3554, 3555, 3556, 3557, 3558, 3559, 3560, 3561, 3562, 3563, 3564, 3565, 3566, 3567, 3568, 3569, 3570, 3571, 3572, 3573, 3574, 3575, 3576, 3577, 3578, 3579, 3580, 3581, 3582, 3583, 3584, 3585, 3586, 3587, 3588, 3589, 3590, 3591, 3592, 3593, 3594, 3595, 3596, 3597, 3598, 3599, 3600, 3601, 3602, 3603, 3604, 3605, 3606, 3607, 3608, 3609, 3610, 3611, 3612, 3613, 3614, 3615, 3617, 3618, 3619, 3620, 3621, 3622, 3623, 3624, 3625, 3626, 3627, 3628, 3629, 3630, 3631, 3632, 3633, 3634, 3635, 3636, 3637, 3638, 3639, 3640, 3641, 3642, 3643, 3644, 3645, 3646, 3647, 3648, 3649, 3650, 3651, 3652, 3653, 3654, 3655, 3656, 3657, 3658, 3659, 3660, 3661, 3662, 3663, 3664, 3665, 3666, 3667, 3668, 3669, 3670, 3671, 3672, 3673, 3674, 3675, 3677, 3678, 3679, 3680, 3681, 3682, 3683, 3684, 3685, 3686, 3687, 3688, 3689, 3690, 3691, 3692, 3693, 3694, 3695, 3696, 3697, 3698, 3699, 3700, 3701, 3702, 3703, 3704, 3705, 3706, 3707, 3708, 3709, 3711, 3712, 3713, 3714, 3715, 3717, 3718, 3719, 3720, 3721, 3722, 3723, 3724, 3725, 3726, 3727, 3728, 3729, 3730, 3731, 3732, 3734, 3735, 3736, 3737, 3738, 3739, 3740, 3741, 3745, 3746, 3747, 3748, 3749, 3750, 3751, 3752, 3753, 3754, 3755, 3757, 3758, 3759, 3760, 3761, 3762, 3763, 3764, 3765, 3766, 3767, 3768, 3769, 3770, 3771, 3772, 3773, 3774, 3775, 3776, 3777, 3778, 3779, 3780, 3781, 3782, 3783, 3784, 3785, 3786, 3788, 3789, 3790, 3791, 3792, 3793, 3794, 3795, 3796, 3797, 3798, 3799, 3800, 3801, 3802, 3807, 3808, 3810, 3811, 3812, 3813, 3814, 3815, 3816, 3817, 3818, 3819, 3820, 3822, 3823, 3824, 3825, 3826, 3827, 3828, 3830, 3831, 7011, 7012, 7032, 7033) then 'HOTEL'
                            when merchant_category_code in (5541, 5542, 4121, 7542, 5511, 5521, 5983, 7523, 7524, 3351, 3354, 3357, 3362, 3368, 3370, 3376, 3385, 3389, 3390, 3394, 3396, 3405, 3412, 3420, 3433, 7512, 7513, 7519, 5531, 5532, 5533, 7534, 7535, 7538) then 'CAR'
                            when merchant_category_code in (8299, 8211, 8220, 8241, 8244, 8249, 8299, 8351) then 'EDUCATION'
                            when merchant_category_code in (5815, 5816, 5817, 5818) then 'DIGITAL GOODS'
                            when merchant_category_code in (5045, 5251, 5271, 5732, 5734, 5946, 7622, 7623, 7629, 7993) then 'TECHNOLOGY'
                            when merchant_category_code in (7832, 7911, 7922, 7929, 7932, 7933, 7994, 7996, 7997, 7998, 7999, 5300, 7992, 7800, 7801, 7802, 7995, 9754) then 'ENTERTAINMENT'
                            when merchant_category_code in (8398, 8641, 8651, 8661, 8675, 8699) then 'ONG'
                            else 'OTHER'
                        end as category,
                        merchant_category_code
                    from new_commerces)

                    SELECT * FROM clean_names
                    WHERE code IS NOT NULL
                )
        """
    )
    # Productive table
    insert_prod = RedshiftSQLOperator(
        task_id = "insert_prod",
        sql = """
                INSERT INTO lemoncash_data.commerces(code, name, category, merchant_category_code)
                SELECT
                        code,
                        name,
                        category,
                        merchant_category_code
                FROM lemoncash_data.commerces_staging_table"""
    )

    # Checks
    check_duplicates = SQLCheckOperator(
        conn_id = "Redshift_Data",
        task_id = "check_duplicates",
        sql = """
                with duplicated_rows as (
                    select count(*) as duplicated_events from (
                        select code, name, category, merchant_category_code, count(*)
                        from lemoncash_data.commerces_staging_table
                        group by 1, 2, 3, 4
                        having count(*) > 1))
                select case when duplicated_events > 0 then 0 else 1 end as duplicated_check
                from duplicated_rows
        """)

    check_nulls = SQLCheckOperator(
        conn_id = "Redshift_Data",
        task_id = "check_nulls",
        sql = """
                with null_rows as (
                    select count(*) as null_events from (
                        select * from lemoncash_data.commerces_staging_table
                        where code is null
                    	or name is null
                    	or category is null
                    	or merchant_category_code is null
                            ))
                select case when null_events > 0 then 0 else 1 end as null_check
                from null_rows
        """)

    delete_staging = RedshiftSQLOperator(
        task_id = "delete_staging",
        sql = """ DROP TABLE lemoncash_data.commerces_staging_table """
    )

create_staging_table>>insert_clean_commerces>>[check_duplicates, check_nulls]>>insert_prod>>delete_staging
