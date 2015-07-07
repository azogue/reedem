# -*- coding: utf-8 -*-
"""
Created on Fri Jun  5 18:16:24 2015
DataBase de datos de consumo eléctrico
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
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import httplib2

from dataweb import DataWeb

# %matplotlib inline

TZ = pytz.timezone('Europe/Madrid')
NAME_DATA_PVPC = 'data_pvpc.h5'
PATH_DATABASE = os.path.join(os.path.dirname(__file__), NAME_DATA_PVPC)

ANYO_INI = 2014  # Inicio de los datos en el origen
DATE_INI = '%lu0401' % ANYO_INI  # es abril 04
DATE_FMT = '%Y%m%d'

FREQ_DATA = '1h'
TS_DATA = 3600  # Muestreo en segundos
DELTA_DATA = np.timedelta64(1, 'h')
NUM_TS_MIN_PARA_ACT = 0  # 1 hora? - > desactivado

NUM_RETRIES = 7
DIAS_MERGE_MAX = 10
MAX_THREADS_MERGE = 500
MAX_THREADS_REQUESTS = 20  # Números altos colapsan el servidor de esios.ree
USAR_MULTITHREAD = True
MAX_ACT_EXEC = 1000


def pvpc_url_dia(str_dia='20150622', dtfmt=DATE_FMT):  # url = 'http://www.esios.ree.es/pvpc/'
    if type(str_dia) is pd.Timestamp:
        str_dia = str_dia.strftime(dtfmt)
    else:
        assert (type(str_dia) == str)
    return 'http://www.esios.ree.es/Solicitar?fileName=pvpcdesglosehorario_' + str_dia + '&fileType=xml&idioma=es'


def pvpc_procesa_datos_dia(response, tz=TZ):
    # Extrae pandas dataframe con valores horarios e info extra del XML PVPC
    try:  # <sinDatos/>
        if len(response) > 0:
            soup_pvpc = BeautifulSoup(response)
            str_horizonte = soup_pvpc.find_all('horizonte')[0]['v']
            ts = pd.Timestamp(pd.Timestamp(str_horizonte.split('/')[1]).date(), tz=tz)
            identseriesall = soup_pvpc.find_all('terminocostehorario')
            data_horas, num_horas = {}, 24
            for serie in identseriesall:
                idunico = serie['v']
                if len(serie.find_all('tipoprecio')) > 0:
                    idunico += '_' + serie.tipoprecio['v']
                values = [float(v['v']) for v in serie.find_all('ctd')]
                num_horas = len(values)
                data_horas[idunico] = np.array(values)
                # str_interv_serie = soup_pvpc.find_all('intervalotiempo')[0]['v']
                # assert(str_interv_serie == str_horizonte)

            dfdata = pd.DataFrame(data_horas, index=pd.DatetimeIndex(start=ts, periods=num_horas, freq='1h'))
            # ident_file = soup_pvpc.find_all('identificacionmensaje')[0]['v']
            # assert(dfdata.index.tz.zone == self.TZ.zone)
            return dfdata, 0
        else:
            # print u'ERROR No se encuentra información de web:'
            return None, -1
    except:
        # print u'ERROR leyendo información de web:'
        return None, -2


def pvpc_data_dia(str_dia='20150622', num_retries=NUM_RETRIES, dtfmt=DATE_FMT, tz=TZ):
    url = pvpc_url_dia(str_dia, dtfmt)
    print 'Intentando descargar datos en: ', url
    count, status, ok, ok_p, response = 0, -1000, False, False, []
    http = httplib2.Http(cache=".cache", timeout=3)
    while count < num_retries and not ok:
        status, response = None, None
        try:
            status, response = http.request(url)
            try:
                if len(response) > 0:
                    print 'STATUS: %s, TAMAÑO: %lu_c' % (str(status['status']), len(response))
                    ok = True
                else:
                    print 'STATUS: %s' % str(status['status'])
                    count += 1
            except:
                print 'Error: empty response? STATUS:', status  # print response
                count += 1
        except:
            print 'ERROR [intento %lu; status: %s]' % ((count + 1), str(status))
            count += 1
    if ok:
        data_dia, ok_p = pvpc_procesa_datos_dia(response, tz)
        try:
            if ok_p == 0:
                assert (type(data_dia) is pd.DataFrame)
            print 'Importada DataFrame de %lu valores [freq=%s; tz=%s]' \
                  % (len(data_dia.index), data_dia.index.freqstr, data_dia.index.tzinfo.zone)
        except:
            print 'ERROR EN PROCESADO de: %s [Intentos: %lu, ok_download: %s, ok_process: %s, len_resp: %luc, url:%s]' \
                  % (str_dia, count + 1, str(ok), str(ok_p), len(response), url)
        return data_dia
    else:
        print 'ERROR EN DESCARGA de: ', url, status, count, response
        return response


class PVPC(DataWeb):
    def __init__(self, forze_update=False, verbose=True):
        super(PVPC, self).__init__(PATH_DATABASE,
                                   u'Datos de: demanda.ree.es',
                                   self.url_data_dia,
                                   self.procesa_data_dia,
                                   forze_update, verbose,
                                   TZ=TZ, DATE_FMT=DATE_FMT, DATE_INI=DATE_INI,
                                   TS_DATA=TS_DATA, NUM_TS_MIN_PARA_ACT=NUM_TS_MIN_PARA_ACT,
                                   FREQ_DATA=FREQ_DATA, DELTA_DATA=DELTA_DATA,
                                   NUM_RETRIES=NUM_RETRIES, MAX_THREADS_REQUESTS=MAX_THREADS_REQUESTS,
                                   DIAS_MERGE_MAX=DIAS_MERGE_MAX, MAX_THREADS_MERGE=MAX_THREADS_MERGE,
                                   MAX_ACT_EXEC=MAX_ACT_EXEC, USAR_MULTITHREAD=USAR_MULTITHREAD)
        # self.errores = self.busca_errores_data(False)
        # self.save_data('errores', self.errores)
        print(self.store.keys())
        print(self.store)

    # noinspection PyMethodMayBeStatic
    def url_data_dia(self, str_dia='20150622'):
        return pvpc_url_dia(str_dia, dtfmt=DATE_FMT)

    # noinspection PyMethodMayBeStatic
    def procesa_data_dia(self, str_dia, dict_data):
        dfdata, status = pvpc_procesa_datos_dia(dict_data[str_dia][1], tz=TZ)
        if status == 0:
            dict_data[str_dia] = dfdata
        return status


#############################
# Main
#############################
def main():
    """
     Actualiza la base de datos de PVPC almacenados como dataframe en local,
     creando una nueva si no existe o hubiere algún problema. Los datos registrados se guardan en HDF5
    """
    data_ej = pvpc_data_dia('20141026')
    datos_pvpc = PVPC(forze_update=False)  # ,False)
    datos_pvpc.close()

    print datos_pvpc.data.index
    return datos_pvpc, datos_pvpc.data


if __name__ == '__main__':
    datos_pvpc, data = main()
    print 'Se devuelven variables: datos_pvpc, data.'


# tocInit = time.time()
#
# DATA = PVPC(False)
# print 'DONE!!!!'
# DATA.info_data()

# dia_ini = pd.Timestamp('2014-04-01')
# dia_fin = pd.Timestamp('today')
# print 'Recolectando PVPC desde:', dia_ini, 'a: ', dia_fin
# dia, iniciado = dia_ini, False
# while dia <= dia_fin:
#    toc = time.time()
#    dfdata = obtiene_datos_pvpc_dia(dia.strftime('%Y%m%d'))
#    if iniciado == True:
#        dcjoin = dcjoin.append(dfdata)
#    else:
#        dcjoin = dfdata
#        iniciado = True
#    toc = printTicIntervalo('Obtención de PVPC: ' + str(dia), toc)
#    dia += np.timedelta64(1,'D')

# dcjoin = DATA.get_data_en_intervalo('20141025','20141025')
# # dcjoin1 = obtiene_datos_pvpc_dia('20141026')
# # dcjoin2 = obtiene_datos_pvpc_dia('20141027')
# # dcjoin = dcjoin0.append(dcjoin1)
# # dcjoin = dcjoin.append(dcjoin2)
#
# print dcjoin.head()
# print dcjoin.tail()
# print dcjoin.info()
# print dcjoin.describe()
# print dcjoin.FEU_Z02
# #dcjoin.FEU_Z02.plot()
#
# def ref_tarifa(ind_tarifa):
#     if ind_tarifa == 1:
#         return ('2.0 A', 'Tarifa por defecto (2.0 A)')
#     elif ind_tarifa == 2:
#         return ('2.0 A', 'Eficiencia 2 periodos (2.0 DHA)')
#     else:
#         return ('2.0 DHS', 'Vehículo eléctrico (2.0 DHS)')
#
#
# def key_tarifa(key, ind_tarifa):
#     if mags_pvpc[key][1] == True:
#         if ind_tarifa == 1:
#             return key + '_Z01'
#         elif ind_tarifa == 2:
#             return key + '_Z02'
#         else:
#             return key + '_Z03'
#     else:
#         return key
#
#
# mags_pvpc = {'FEU'   : (u'Término energía PVPC', True),
#              'CAPh'  : (u'Cargo capacidad', True),
#              'CCOMh' : (u'Financiación OM', False),
#              'CCOSh' : (u'Financiación OS', False),
#              'CDSVh' : (u'Coste desvíos', False),
#              'INTh'  : (u'Servicio de interrumpibilidad', False),
#              'OCh'   : (u'Total OC', True),
#              'PMASh' : (u'Otros sistema', False),
#              'Pmh'   : (u'Total PMH', False),
#              'SAh'   : (u'Total SAH', False),
#              'TCUh'  : (u'Precio producción', True)}
#
# mags_web = {'FEU'   : (u'Término energía PVPC', True),
#              'CP'   : (u'Costes de producción', True),
#              'mercado'  : (u'Mercado diario e intradiario', True),
#              'ajuste' : (u'Servicios de ajuste', False),
#              'peaje_acceso' : (u'Peaje de acceso', False),
#              'pago_capacidad' : (u'Pago por capacidad', False),
#              'serv_inint'  : (u'Servicio de interrumpibilidad', False),
#              'OS'   : (u'Financiación OM', True),
#              'OM' : (u'Financiación OS', False)}
#
#
# ind_tarifa = 2
# keys = [key_tarifa(k, ind_tarifa) for k in mags_pvpc.keys()]
# print keys
# pvpc_data = dcjoin[keys]
# print pvpc_data.ix['2014-10-25 20:00:00']
#
# AAC = (pvpc_data.FEU_Z02 - pvpc_data.TCUh_Z02)
# CP = (pvpc_data.SAh + pvpc_data.Pmh + pvpc_data.OCh_Z02)
# PERD = pvpc_data.TCUh_Z02 / CP - 1
# MERC = pvpc_data.Pmh * (1+PERD)
# AJUS = pvpc_data.SAh  * (1+PERD)
# CAP = (pvpc_data.OCh_Z02 - pvpc_data.CCOMh - pvpc_data.CCOSh - pvpc_data.INTh) * (1+PERD)
#
# pvpc_data['mercado'] = MERC
# pvpc_data['ajuste'] = AJUS
# pvpc_data['acceso'] = AAC.copy()
# pvpc_data['capacidad'] = CAP
# pvpc_data['sint'] = pvpc_data.INTh  * (1+PERD)
# pvpc_data['fos'] = pvpc_data.CCOSh  * (1+PERD)
# pvpc_data['fom'] = pvpc_data.CCOMh  * (1+PERD)
#
# print pvpc_data.ix['2014-10-25 20:00:00']
#
# #plt.style.use('ggplot')
# plt.style.context('fivethirtyeight')
#
# lista = ['mercado','ajuste','acceso','capacidad','sint','fos', 'fom']
# colors = ['0C212C', '103B52', '103B52', '236E9B', '5193B2', '89B6CC', 'C4DAE4']
# #pd.DataFrame.
# fig, ax = plt.subplots(1,1)
# pvpc_data.plot(y=key_tarifa('FEU', ind_tarifa), ax=ax, lw=2, color='#1C5A8B')
# ax.hold(True)
# pvpc_data.plot(y=lista[::-1], ax=ax, kind='area', color=['#'+c for c in colors])
# plt.show()
#
# # ax.annotate('Test', (mdates.date2num(x[1]), y[1]), xytext=(15, 15),
# #             textcoords='offset points', arrowprops=dict(arrowstyle='-|>'))
# #
# # fig.autofmt_xdate()
# # descrColumnas = {'Dia','Hora','Peaje','Periodo',...
# #               'FEU','TEU','TCU',...
# #               'PERD_PVPC','PERD_STD','CP','OC',...
# #               'OS','OM','Cargo_cap','Serv_interr',...
# #               'SAH','Otros_sistema','Coste_desvios',...
# #               'Coste_banda','Coste_reserva','Coste_restr_tec',...
# #               'PMH','Comp_intradia','Mercado_diario',...
# #               'NULL','Coef_perfilado'};
# # descrColumnas = {'Día','Hora','Peaje','Periodo',...
# #               'FEU','TEU','TCU',...
# #               'PERD_PVPC','PERD_STD','CP','Total OC',...
# #               'OS','OM','Cargo capacidad','Servicio interrumpibilidad',...
# #               'Total SAH','Otros sistema','Coste desvíos',...
# #               'Coste banda','Coste reserva','Coste restricciones técnicas diario',...
# #               'Total PMH','Componente intradiario 1','Mercado diario',...
# #               '','Coeficiente perfilado'};
# # udsColumnas = [{'','h','','',...
# #               '?/MWh consumo','?/MWh consumo','?/MWh consumo',...
# #               '%','%'},rellena({'?/MWh bc'},15), {'',''}];
# # plt.plot(dcjoin.FEU_Z02)
# # plt.show()
