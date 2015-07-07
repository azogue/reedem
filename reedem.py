# -*- coding: utf-8 -*-
"""
Gestión de datos de demanda energética proporcionados en la web de REE.es
@author: Eugenio Panadero
"""
__author__      = 'Eugenio Panadero'
__copyright__   = "Copyright 2015, AzogueLabs"
__credits__     = ["Eugenio Panadero"]
__license__     = "GPL"
__version__     = "1.0"
__maintainer__  = "Eugenio Panadero"

import re
import datetime as dt

import pandas as pd

from dataweb import DataWeb
from reedemconf import *
import reedemplot as rdp


#import matplotlib.pyplot as plt
#%matplotlib inline

# TODO usar progreso en reedem
#import pyprogres as prg

class DatosREE(DataWeb):
    def __init__(self, forze_update=False, verbose=True):
        super(DatosREE, self).__init__(PATH_DATABASE,
                                       u'Datos de: demanda.ree.es',
                                       self.url_data_dia, self.procesa_data_dia,
                                       forze_update, verbose,
                                       TZ=TZ, DATE_FMT=DATE_FMT, DATE_INI=DATE_INI,
                                       TS_DATA=TS_DATA, NUM_TS_MIN_PARA_ACT=NUM_TS_MIN_PARA_ACT,
                                       FREQ_DATA=FREQ_DATA,DELTA_DATA=DELTA_DATA,
                                       NUM_RETRIES=NUM_RETRIES,MAX_THREADS_REQUESTS=MAX_THREADS_REQUESTS,
                                       DIAS_MERGE_MAX=DIAS_MERGE_MAX,MAX_THREADS_MERGE=MAX_THREADS_MERGE)
        self.errores = self.busca_errores_data(False)
        self.save_data('errores', self.errores)
        print(self.store.keys())
        print(self.store)

    def url_data_dia(self, str_dia):
        if type(str_dia) is pd.Timestamp:
            str_dia = str_dia.strftime(DATE_FMT)
        else:
            assert(type(str_dia) == str)
        return 'https://demanda.ree.es/' + 'WSvisionaMoviles' + 'PeninsulaRest' + '/resources/demanda' + 'GeneracionPeninsula' \
               + '?callback=angular.callbacks._2&curva=' + 'DEMANDA' + '&fecha=' + str_dia

    def procesa_data_dia(self, str_dia, dict_data):

        def _limpia_string_timestamp(sufijo_tz, cambio_dst, sufijo_tz_next, ts_bruto):
            if cambio_dst:
                if ts_bruto.find('A') > 0:
                    ts_limpio = ts_bruto.replace('A', '') + sufijo_tz
                elif ts_bruto.find('B') > 0:
                    ts_limpio = ts_bruto.replace('B', '') + sufijo_tz_next
                else:
                    ts_limpio = ts_bruto + pd.Timestamp(ts_bruto,tz=TZ).strftime(' %z')
            else:
                ts_limpio = ts_bruto + sufijo_tz
            return pd.Timestamp(ts_limpio,tz=TZ)

        tsdia = pd.Timestamp(str_dia,tz=TZ)
        strtz, strtz_next = tsdia.strftime(' %z'), (tsdia + dt.timedelta(2)).strftime(' %z')
        hay_cambio_dst = strtz_next != strtz
        rg_contenido = re.compile(r'angular\.callbacks\._2\(\{(.*?)\}\);')
        try:
            str_content = rg_contenido.findall(dict_data[str_dia][1])[0]
            if len(str_content) > 0:
                # TODO Revisar DANGER EVAL() = EVIL()
                data = pd.DataFrame(eval(str_content[28:]),dtype=np.int64)
                data.set_index(pd.DatetimeIndex([_limpia_string_timestamp(strtz, hay_cambio_dst, strtz_next, ts)
                                                 for ts in data.ts],freq=FREQ_DATA),inplace=True)
                data.drop(['ts'],axis=1,inplace=True)
                dict_data[str_dia] = data
                return 0
            else:
                print u'ERROR No se encuentra información de web:', str_dia, dict_data[str_dia][1]
                return -1
        except:
            print u'ERROR leyendo información de web:', dict_data[str_dia][1]
            return -2


    def busca_errores_data(self,verbose=True):
        self.append_delta_index(TS_DATA)
        data = self.data
        idx_desconex = data.delta_T > 1
        fechas_desconex = [ed.to_datetime() for ed in data.index[idx_desconex].tolist()]

        errores = pd.DataFrame(data={'anyo': [f.year for f in fechas_desconex],
                                     'mes': [f.month for f in fechas_desconex],
                                     'dia': [f.day for f in fechas_desconex],
                                     'hora': [f.hour for f in fechas_desconex],
                                     'dia_semana': [f.weekday() for f in fechas_desconex],
                                     'ordinal': [f.toordinal() for f in fechas_desconex],
                                     'str_dia': [f.strftime(DATE_FMT) for f in fechas_desconex],
                                     'delta_T': data.delta_T[idx_desconex]},
                               index= data.index[idx_desconex],
                               columns=['anyo','mes','dia','hora','dia_semana','ordinal','str_dia','delta_T'])
        try:
            errores_anyo = errores[dt.date.today().strftime('%Y')]
        except:
            errores_anyo = []
        if verbose:
            fig, hejes = rdp.get_lienzo()
            errores[errores.anyo != 2009].hist(ax=hejes)

            print 'Hay %lu errores en la base de datos, producidos en %lu días (%lu de este año).\nEl último fue en %s, con un delta_Ts de %lu' \
                  % (len(errores), len(set(errores.ordinal)), len(errores_anyo), errores.index[-1], errores.delta_T[-1])

            dias_errores = sorted(set(errores.str_dia))
            if len(errores_anyo) > 0:
                print 'Este año hay errores:', errores_anyo
            print 'Se han detectado errores en los días:', dias_errores
        return errores


    def plot_prod_vs_dem(self, prodtot=None, demanda=None):
        # Plot Producción total vs Demanda
        datos_plot = data if data is not None else self.data
        # TODO Arreglar plot prod vs dem
        rdp.plot_prod_vs_dem(prodtot, demanda)

    def plot_ajuste_prod_dem(self, prodtot=None, exceso=None):
        # Plot Ajuste entre Producción total y Demanda
        datos_plot = data if data is not None else self.data
        # TODO Arreglar plot prod vs dem
        rdp.plot_ajuste_prod_dem(prodtot, exceso)

    def plot_tipos_prod(self, data=None, cols_prod=COLS_PROD):
        # Plot Producción por tipos
        datos_plot = data if data is not None else self.data
        rdp.plot_tipos_prod(datos_plot, cols_prod)

    def plotarea_stack_tipos_prod(self, data=None, cols_prod=COLS_PROD):
        # Plot Stack Producción por tipos
        datos_plot = data if data is not None else self.data
        rdp.plotarea_stack_tipos_prod(datos_plot, cols_prod)

    def plot_dia_raro(self, data=None, str_dia = '2007-11-24'):
        # Plot día raro (datos incorrectos, desconexiones, etc.)
        datos_plot = data if data is not None else self.data
        rdp.plot_dia_raro(datos_plot, str_dia)

    def plot_renov(self, data=None):
        # Plot renovables (sin eólica)
        datos_plot = data if data is not None else self.data
        rdp.plot_renov(datos_plot)

    def plot_resample(self, data=None, freq='1M', how=np.mean, kind='line', tupla_mag=None):
        # Plot de resample datos'
        # how=[np.min, np.max, np.mean]
        datos_plot = data if data is not None else self.data
        rdp.plot_resample(datos_plot,freq,how,kind,tupla_mag)


    # dias_raros = ['2007-11-24','2008-01-01','2009-10-06','2011-01-07']
    # for d in dias_raros:
    #     plot_dia_raro(data, d)



#############################
# Main
#############################
def main():
    """
     Actualiza la base de datos de demanda y producción eléctrica de REE almacenados como dataframe en local,
     creando una nueva si no existe o hubiere algún problema. Los datos registrados se guardan en HDF5
    """
    datos_ree = DatosREE(forze_update=False)#,False)
    datos_ree.close()
    return datos_ree, datos_ree.data


if __name__ == '__main__':
    datos_ree, data = main()
    print 'Se devuelven variables: datos_ree, data.'
