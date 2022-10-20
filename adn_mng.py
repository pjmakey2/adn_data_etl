#coding: latin-1
import pandas as pd
import os, random, math
import logging

MAP_CURRENCY = {
    u'DOLAR ESTADOUNIDENSE': 'USD',
    u'REAL': 'BRL',
    u'MONEDA COMUN EUROPEA': 'EUR',
    u'nan': 'ND',
    u'FRANCOS SUIZOS': 'CHF',
    u'YENS': 'JPY',
    u'LIRAS ITALIANAS': 'ITL',
    u'GUARANI': 'GS',
    u'CORONAS SUECAS': 'SEK',
    u'LIBRA ESTERLINA': 'GBP',
    u'YUAN CHINO': 'CNY',
    u'DOLAR DE SINGAPUR': 'SGD',
    u'PESOS': 'ARS',
    u'DOLAR DE HONG KONG': 'HKD',
    u'QUETZAL': 'GTQ',
    u'PESOS MEJICANOS': 'MXN',
    u'CORONAS NORUEGAS': 'NOK',
    u'ESCUDOS PORTUGUESES': 'PTE',
    u'DOLAR AUSTRALIANO': 'AUD',
    u'DOLAR CANADIENSE': 'CAD',
}
K_ORDER = [
    u'OFICIALIZACION',
    u'CANCELACION',    
    u'ADUANA',
    u'OPERACION',
    u'DESPACHO',
    u'RUBRO',    
    u'CONOCIMIENTO',
    u'DESTINACION',
    u'REGIMEN',
    u'ESTADO',
    u'DESPACHANTE',
    u'RUC DESPACHANTE',
    u'DATOS DESPACHANTE',
    u'EMAIL DESPACHANTE',
    u'IMPORTADOR',    
    u'RUC IMPORTADOR',
    u'EMAIL IMPORTADOR',
    u'FACTURA',
    u'DESC CAPITULO',
    #u'DESC PARTIDA',
    #u'DESC POSICION',
    u'MERCADERIA',
    u'ACUERDO',    
    u'COTIZACION',
    u'PROVEEDOR',
    u'MEDIO TRANSPORTE',
    u'CANAL',
    u'POSICION ARANCELARIA',    
    u'POS1',
    u'POS2',
    u'POS3',    
    u'POS4',
    u'ITEM',
    u'PAIS ORIGEN',
    u'PAIS DESTINO/PROCEDENCIA',
    u'USO',
    u'UNIDAD MEDIDA',
    u'CANTIDAD',    
    u'VALOR FACTURA',    
    u'KILO NETO',
    u'KILO BRUTO',
    u'FOB DOLAR',
    u'FLETE DOLAR',
    u'SEGURO DOLAR',
    u'IMPONIBLE DOLAR',
    u'IMPONIBLE GS',
    u'AJUSTE A INCLUIR',
    u'AJUSTE A DEDUCIR',    
    u'NUMERO SUBITEM',
    u'CANTIDAD SUBITEM',
    u'PRECION UNITARIO SUBITEM',
    u'DESC SUBITEM',
    u'MARCA SUBITEM',
    u'DERECHO',
    u'ISC',
    u'SERVICIO',
    u'RENTA',
    u'IVA',
    u'TOTAL',
    u'MONEDA',

]

ST_CLEAN = [
    u'ADUANA',
    u'OPERACION',
    u'DESPACHO',
    u'RUBRO',
    u'CONOCIMIENTO',
    u'DESTINACION',
    u'REGIMEN',
    u'ESTADO',
    u'DESPACHANTE',
    u'RUC DESPACHANTE',
    u'DATOS DESPACHANTE',
    u'EMAIL DESPACHANTE',
    u'IMPORTADOR',    
    u'RUC IMPORTADOR',
    u'EMAIL IMPORTADOR',
    u'FACTURA',
    u'DESC CAPITULO',
    u'MERCADERIA',
    u'ACUERDO',    
    u'PROVEEDOR',
    u'MEDIO TRANSPORTE',
    u'CANAL',
    u'PAIS ORIGEN',
    u'PAIS DESTINO/PROCEDENCIA',
    u'USO',
    u'UNIDAD MEDIDA',
    u'DESC SUBITEM',
    u'MARCA SUBITEM',
]

SEP_I = '  - '
def extract_currency(x):
    if unicode(x).find(SEP_I) > 0:
        for a in  x.split(SEP_I):
            return a.split('(')[-1].strip().strip('()')
    return x

def deconstr_arancel(x):
    return x.split('.')[0:4]
    

def generate_import_export_fcube(workdir, year):
    fpath = '{}/{}'.format(workdir, year)
    logging.info('Lis folder {}'.format(fpath))
    for ff in os.listdir(fpath):
        fname = '{}/{}'.format(fpath, ff)
        logging.info('Reading file {}'.format(fname))
        dfn = pd.read_csv(fname, sep=';', 
                                 quotechar='"', 
                                 encoding='latin-1', 
                                 decimal=',', 
                                 dtype={'VALOR FACTURA': float,
                                        'COTIZACION': float,
                                        'CANTIDAD': float,
                                        'KILO NETO': float,
                                        'KILO BRUTO': float,
                                        'FOB DOLAR': float,
                                        'FLETE DOLAR': float,
                                        'SEGURO DOLAR': float,
                                        'IMPONIBLE DOLAR': float,
                                        'IMPONIBLE GS': float,
                                        'AJUSTE A INCLUIR': float,
                                        'AJUSTE A DEDUCIR': float,
                                        'NUMERO SUBITEM': float, #
                                        'CANTIDAD SUBITEM': float,
                                        'PRECION UNITARIO SUBITEM': float,
                                        'DERECHO': float,
                                        'ISC': float,
                                        'SERVICIO': float,
                                        'RENTA': float,
                                        'IVA': float,
                                        'TOTAL': float,
                                 })
        dfn.fillna(value=0, inplace=True)
        logging.info('Format date values')
        dfn['OFICIALIZACION'] = pd.to_datetime(dfn['OFICIALIZACION'], format='%d/%m/%y')
        dfn['CANCELACION'] = pd.to_datetime(dfn['CANCELACION'], format='%d/%m/%y')
        logging.info('Mapping currency fields')
        dfn['MONEDA'] = dfn['FACTURA'].apply(extract_currency)
        dfn['MONEDA'] = dfn['MONEDA'].map(MAP_CURRENCY).fillna(dfn['MONEDA'])
        dfn['FACTURA'] = dfn['FACTURA'].str.replace('\([A-Z\s]+\)', '').str.replace('\s+', '')
        for FIELD in ST_CLEAN:
            logging.info('Cleanning up strings fields from new line {}'.format(FIELD))
            dfn[FIELD] = dfn[FIELD].str.replace('\n', '').str.replace('"', '')
        logging.info('Decouple type of operation')
        dfn['POS4'] = 0
        dfn['POS3'] = 0
        dfn['POS2'] = 0
        dfn['POS1'] = 0
        dfn[['POS4', 'POS3', 'POS2', 'POS1']] = pd.DataFrame(list(dfn['POSICION ARANCELARIA'].astype(unicode).apply(deconstr_arancel)),
                                                                      columns=['POS4', 'POS3', 'POS2', 'POS1'])
        per = dfn['CANCELACION'].dt.to_period("M")
        agg = dfn.groupby([per])
        logging.info('Group and iterate by DATE')
        for year, group in agg:
            datep = str(year).replace('-', '')
            rdid = random.randrange(
                        random.randint(3, 333), 
                        math.pow(random.randint(333, 999), 3)
                   )
            flname = '{}/output/adn_data_{}_{}.csv'.format(workdir, datep, rdid)
            logging.info('Saving file {} for creation of the cube'.format(flname))
            group[K_ORDER].to_csv(flname, sep='|', quotechar='"', encoding='latin-1', index=False)
    return {'success': 'Done!!'}
