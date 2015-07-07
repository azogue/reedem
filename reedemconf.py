# -*- coding: utf-8 -*-
"""
Gestión de datos de demanda energética proporcionados en la web de REE.es
Datos de configuración
@author: Eugenio Panadero
"""
__author__ = 'Eugenio Panadero'
__copyright__ = "Copyright 2015, AzogueLabs"
__credits__ = ["Eugenio Panadero"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Eugenio Panadero"

import os

import pytz

import numpy as np

#############################
# Constantes y métodos
#############################
TZ = pytz.timezone('Europe/Madrid')
NAME_DATA_REE = 'data_ree.hf5'
PATH_DATABASE = os.path.join(os.path.dirname(__file__), NAME_DATA_REE)

ANYO_INI = 2007  # Inicio de los datos en el origen
DATE_INI = '%lu-01-01' % ANYO_INI
DATE_FMT = '%Y-%m-%d'

FREQ_DATA = '10min'
TS_DATA = 600  # Muestreo en segundos
DELTA_DATA = np.timedelta64(10, 'm')
NUM_TS_MIN_PARA_ACT = 0  # 1 hora? - > desactivado
# rexpr = re.compile(r'(\d+)(\w+)')
# DELTA_DATA = np.timedelta64(int(rexpr.findall(FREQ_DATA)[0][0]), (rexpr.findall(FREQ_DATA)[0][1])[0])

NUM_RETRIES = 5
DIAS_MERGE_MAX = 10
MAX_THREADS_MERGE = 500
MAX_THREADS_REQUESTS = 175

FIGSIZE = (12, 6)
DPI = 225
MARGIN_AXIS = [0, 0, 1, 1]

# cols_prod = [u'inter', u'hid', u'icb', u'nuc', u'gf', u'car', u'cc', u'eol', u'solTer', u'solFot', u'termRenov', u'aut']#, u'sol', u'dem']
COLS_PROD = [u'nuc', u'gf', u'car', u'cc', u'hid', u'icb', u'eol', u'solTer', u'solFot', u'termRenov', u'aut',
             u'inter']  # , u'sol', u'dem']

DIAS_RAROS = ['2007-11-24', '2008-01-01', '2009-10-06', '2011-01-07']

COLORES_ENER = {
    "inter": '#E0D8AA',
    "hid": '#0480CC',
    "icb": '#FF5729',
    "nuc": '#40378B',
    "gf": '#B20919',
    "car": '#A45532',
    "cc": '#FFC859',
    "eol": '#5BA80B',
    "solTer": '#FF0000',
    "solFot": '#E07B00',
    "termRenov": '#8B00FF',
    "aut": '#0D9943',
    "sol": 'y'}

COLORES_ENER_FONDO = {
    "nuc": '#0D9943',
    "gf": '#0D9943',
    "car": '#0D9943',
    "cc": '#0D9943',
    "el": '#0D9943',
    "hid": '#0D9943',
    "aut": '#0D9943',
    "inter": '#0D9943',
    "sol": '#0D9943',
    "icb": '#0D9943',
    "solTer": '#FF3138',
    "solFot": '#E89A34',
    "termRenov": '#A717FF',
    "cogenResto": '#3DB46F'}

TIPOS_ENER = {
    "nuc": u"Nuclear",
    "gf": u"Fuel/gas",
    "car": u"Carbón",
    "cc": u"Ciclo combinado",
    "eol": u"Eólica",
    "hid": u"Hidráulica",
    "aut": u"Resto reg.esp.",  # "aut": u"Cogeneración y resto"
    "inter": u"Intercambios int",
    "sol": u"Solar",
    "icb": u"Enlace balear",
    "solFot": u"Solar fotovoltaica",
    "solTer": u"Solar térmica",
    "termRenov": u"Térmica renovable"}
# #"cogenResto": u"Cogeneración y resto"}
# "die": "Motores diesel",
# "gas": "Turbina de gas",
# "cc": "Ciclo combinado",
# "cb": "Enlace peninsular",
# "tnr": "Resto reg.esp.",
# "eol": "Eólica",
# "emm": "Enlace interislas",
# "fot": "Solar fotovoltaica",
#
# "ele": "Eléctrica",
# "vap": "Turbina de vapor",
# "solar_termica": "Solar térmica"}


# dias_raros = ['2007-11-24','2008-01-01','2009-10-06','2011-01-07']
# for d in dias_raros:
#     plot_dia_raro(data, d)
