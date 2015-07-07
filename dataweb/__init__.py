# -*- coding: utf-8 -*-
"""
Gestión de datos recogidos en web de forma periódica
@author: Eugenio Panadero
"""
__author__ = 'Eugenio Panadero'
__copyright__ = "Copyright 2015, AzogueLabs"
__credits__ = ["Eugenio Panadero"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Eugenio Panadero"

import threading
import time
import datetime as dt
import httplib
import urllib2
import socket

import pytz
import numpy as np
import pandas as pd
import httplib2


class DataWeb(object):
    def __init__(self,
                 path_database='store.h5',
                 titulo=u'Datos de: WEB XXXXXX',
                 func_url_data_dia=None,
                 func_procesa_data_dia=None,
                 forzar_update=False, verbose=True,
                 **kwargs):
        # Init objeto
        self.PATH_DATABASE = path_database  # self.data_ree = []
        self.store = pd.HDFStore(path_database)
        self.titulo = titulo
        self.verbose = verbose
        self.data = None
        self.num_requests_open = 0
        self.keys_attrs = [k for k in kwargs]
        [setattr(self, k, kwargs[k]) for k in self.keys_attrs]

        # BORRAR
        # self.MAX_ACT_EXEC = 1000
        self._init_attr('USAR_MULTITHREAD', True)
        self._init_attr('TZ', pytz.timezone('Europe/Madrid'))
        self._init_attr('DATE_FMT', '%Y-%m-%d')
        self._init_attr('DATE_INI', '%lu-01-01' % 2015)

        self._init_attr('NUM_TS_MIN_PARA_ACT', 1)  # 1 hora? - > desactivado
        self._init_attr('TS_DATA', 600)  # Muestreo en segundos. (1h)
        self._init_attr('FREQ_DATA', '10min')
        self._init_attr('DELTA_DATA', np.timedelta64(10, 'm'))

        self._init_attr('NUM_RETRIES', 5)
        self._init_attr('MAX_THREADS_REQUESTS', 175)
        self._init_attr('DIAS_MERGE_MAX', 10)
        self._init_attr('MAX_THREADS_MERGE', 500)

        if func_url_data_dia is not None:
            self.url_data_dia = func_url_data_dia
        if func_procesa_data_dia is not None:
            self.procesa_data_dia = func_procesa_data_dia

        # Inicia / update de la base de datos
        self.update_data(forzar_update)
        # Grabación de la base de datos hdf en disco (local)
        self.save_data()

    # you want to override this on the child classes
    def url_data_dia(self, str_dia):
        raise NotImplementedError

    # you want to override this on the child classes
    def procesa_data_dia(self, str_dia, dict_data):
        raise NotImplementedError

    def get_data_en_intervalo(self, d0=None, df=None):
        """
        Obtiene los datos en bruto de la red realizando múltiples requests al tiempo
        Procesa los datos en bruto obtenidos de la red convirtiendo a Pandas DataFrame
        """

        def _date(dia):
            if dia is None:
                return dt.date.today()
            elif type(dia) is not dt.date:
                return dt.datetime.strptime(dia, self.DATE_FMT).date()
            else:
                return dia

        def _merge_datos_dias(key_tarea, dict_merge):
            dict_merge[key_tarea] = merge_lista_df(dict_merge[key_tarea])

        def _procesa_merge_datos_dias(lista_dias, dict_data, tz=None):
            if len(lista_dias) > 1 and self.USAR_MULTITHREAD:
                lista_grupos, lista_grupos_dias = list(), [lista_dias[i:i + self.DIAS_MERGE_MAX] for i in
                                                           np.arange(0, len(lista_dias), self.DIAS_MERGE_MAX)]
                for grupo in lista_grupos_dias:
                    lista_dfs = list()
                    for dia in grupo:
                        lista_dfs.append(dict_data[dia])
                    lista_grupos.append(lista_dfs)
                num_grupos = len(lista_grupos)
                keys_grupos = np.arange(num_grupos)
                dict_merge = dict(zip(keys_grupos, lista_grupos))
                self.procesa_tareas_paralelo(_merge_datos_dias, dict_merge, keys_grupos,
                                             u'\nMERGE DATAFRAMES DE DATOS WEB DIARIOS (%lu GRUPOS)',
                                             self.MAX_THREADS_MERGE)
                dict_merge_final = {0: [dict_merge[k] for k in dict_merge.keys()]}
                _merge_datos_dias(0, dict_merge_final)
                df0 = dict_merge_final[0]
            else:
                df0 = merge_lista_df(dict_data.values())
            corrige_tz_si_procede(df0, tz)
            return df0

        tic_ini = time.time()
        lista_dias = [dia.strftime(self.DATE_FMT) for dia in pd.date_range(_date(d0), _date(df))]

        # BORRAR
        if hasattr(self, 'MAX_ACT_EXEC'):
            lista_dias = lista_dias[:self.MAX_ACT_EXEC]

        num_dias = len(lista_dias)
        dict_data = dict(zip(lista_dias, np.zeros(num_dias)))

        # IMPORTA DATOS Y LOS PROCESA
        self.procesa_tareas_paralelo(self._obtiene_data_dia, dict_data, lista_dias,
                                     u'\nPROCESADO DE DATOS WEB DE %lu DÍAS', self.MAX_THREADS_REQUESTS)

        # Comprueba datos descargados:
        # hay_errores = False
        data_es_none = [v is None for v in dict_data.values()]
        if any(data_es_none):
            # hay_errores = True
            print u'HAY TAREAS NO REALIZADAS:'
            lkeys = [v for v in dict_data.keys()]
            # TODO Warning:(147, 73) Class 'Iterable' does not define '__getitem__', so the '[]' operator cannot be used on its instances
            keys_error = [lkeys[ind] for ind in np.nonzero(data_es_none)[0]]
            print keys_error

        # MERGE DATOS
        try:
            data = _procesa_merge_datos_dias(lista_dias, dict_data, self.TZ)
        except:
            print 'FALLA MERGE DATOS. SE FORMA LISTA DE DF''s'
            # hay_errores = True
            data = [v for v in dict_data.values()]
        try:
            self.integridad_data(data)
        except:
            # hay_errores = True
            print 'assert INTEGRIDAD!!'
            try:
                print data.head(), data.tail()
                data.to_hdf('deb_data.h5', 'table', append=False)
            except:
                print 'No se salva para debug!!!!'
        if self.verbose:
            print u'\n\n%lu DÍAS IMPORTADOS [PROCESO TOTAL %.2f seg, %.4f seg/día]' % (
                num_dias, time.time() - tic_ini, (time.time() - tic_ini) / float(num_dias))
        return data

    def actualiza_datos(self, data_ant=None, tmax=None):
        if data_ant is None:
            data = self.get_data_en_intervalo(self.DATE_INI)
        else:
            now = dt.datetime.now(tz=self.TZ)
            delta = int(np.ceil((now - tmax).total_seconds() / self.TS_DATA))
            if delta > self.NUM_TS_MIN_PARA_ACT:
                d0, df = tmax.date(), now.date()
                data_new = self.get_data_en_intervalo(d0, df)
                if self.verbose:
                    print 'NUEVA INFORMACIÓN: (aprox: %lu valores, de %s a %s)' \
                          % (delta, d0.strftime(self.DATE_FMT), df.strftime(self.DATE_FMT))
                    self.info_data(data_new)
                data = merge_lista_df([data_ant, data_new])
                corrige_tz_si_procede(data, self.TZ, True)
            else:
                print 'LA INFORMACIÓN ESTÁ ACTUALIZADA (delta = %.1f segs)' % (now - tmax).total_seconds()
                data = data_ant
        return data

    def update_data(self, forzar_update=False):
        # 1. Check/Lectura de base de datos hdf en disco (local)
        try:
            if forzar_update:
                if self.verbose:
                    print '''Se procede a actualizar TODOS los datos (force update ON)'''
                assert ()
            data_ant = self.load_data()
            tmax = data_ant.index[-1].to_datetime()
            if self.verbose:
                print '''\nBASE DE DATOS LOCAL HDF:\n\tNº entradas:\t%lu mediciones\n\tÚltima:\t\t%s''' % (
                    len(data_ant), tmax.strftime('%d-%m-%Y %H:%M'))
        except:
            if forzar_update:
                print u'NO SE PUEDE LEER LA BASE DE DATOS LOCAL HDF'
            data_ant, tmax = None, None
            if self.verbose:
                print 'Se procede a realizar la captura de TODOS los datos existentes:'
        # 2. Actualización de la base de datos
        data = self.actualiza_datos(data_ant, tmax)
        # 2.5. Info DataFrame
        if self.verbose:
            print '\n\nINFORMACIÓN EN BASE DE DATOS:'
            self.info_data(data)
        self.data = data

    def obtiene_data_url(self, url, key, dict_data, func_process_data, num_retries=3, http=None):

        def _print_exception(str_e, e, url, key):
            print 'EXCEPCIÓN!!:', str(e)
            try:
                print '''[%s] en [args:%s, message:%s]''' % (str_e, e.args, e.message)  # strerror:%s,
            except:
                print '''[%s] de type: [%s]''' % (str_e, type(e))
            print '''Reintentando [%s] con key: [%s]''' % (url, key)
            pass

        if http is None:
            http = httplib2.Http(cache=".cache", timeout=3)  # (timeout=5)#cache=".cache",
        count = 0
        while count < num_retries:
            self.num_requests_open += 1
            try:
                dict_data[key] = http.request(url)
                try:
                    ok = func_process_data(key, dict_data)
                except:
                    print 'EUREKA!????'
                    ok = -3
                # , len(out)
                if ok != 0:
                    if count > 1:
                        print 'Reintentando (%luº) obtener los datos de %s' % (count + 1, key)
                    count += 1
                else:
                    self.num_requests_open -= (count + 1)
                    break
            except httplib.ResponseNotReady as e:
                # TODO wait con ResponseNotReady??
                if count > 1:
                    print '%luº EXCEPCIÓN: ResponseNotReady [%s: %s,%s]' % (count + 1, type(e), key, url)
                count += 1
            except socket.error as e:  # [Errno 60] Operation timed out
                # TODO qué hacer con un socket.error 60 Operation timed out??
                # print '''Error [%s:%s] en key: %s [%s]''' % (e.errno, e.strerror, key, url)
                if count > 1:
                    print '%luº EXCEPCIÓN: 60 Operation timed out?? [%s]' % (count + 1, type(e))
                count += 1
            except (httplib2.HttpLib2Error, httplib2.HttpLib2ErrorWithResponse, urllib2.URLError) as e:
                if count > 1:
                    print type(e)  # catched
                    _print_exception('httplib2.HttpLib2Error?, httplib2.HttpLib2ErrorWithResponse?, urllib2.URLError?',
                                     e, url, key)
                count += 1
                # TODO except error: [Errno 60] Operation timed out; ResponseNotReady
            except TypeError as te:
                _print_exception('TypeError', te, url, key)
                count += 1
            except Exception as exc:
                if count > 0:
                    print '%luº Exception no reconocida: %s!!' % (count + 1, type(exc))
                    _print_exception('???', exc, url, key)
                count += 1

        if count > 0 and count == num_retries:
            print 'NO SE HA PODIDO OBTENER LA INFO EN %s' % url
        elif count > 0:
            print '--> OK (intentos: %lu; num_requests_open: %lu)' % (count + 1, self.num_requests_open)

    def _obtiene_data_dia(self, str_dia, dict_data, http=None):
        return self.obtiene_data_url(self.url_data_dia(str_dia), str_dia, dict_data,
                                     self.procesa_data_dia, self.NUM_RETRIES, http)

    def _init_attr(self, key, valor):
        if not hasattr(self, key):
            setattr(self, key, valor)

    def integridad_data(self, data=None):
        if data is None:
            data = self.data
        assert (data.index.is_unique and data.index.is_monotonic_increasing and data.index.is_all_dates)

    def info_data(self, data=None, completo=True):
        if data is None:
            data = self.data
        if completo:
            print '\n', data.info(), '\n', data.describe(), '\n'
        print data.head()
        print data.tail()

    def save_data(self, key_data=None, dataframe=None):
        """ Guarda en disco la información y devuelve PATH_DATABASE"""
        assert (self.store.is_open)
        if key_data is None:
            key = 'data'
            data = self.data
        else:
            key = key_data
            data = dataframe
        print 'SALVANDO INFO en key=%s:' % key
        self.integridad_data(data)
        # print data
        # if key in self.store.keys():
        #     del self.store[key]
        # self.data.to_hdf(self.PATH_DATABASE, format='t', mode='w')#, complib='zlib', complevel=9, fletcher32=True)
        # self.store.append(key, self.data, format='table', mode='w')
        self.store.put(key, data, format='table', mode='w')
        self.store.close()
        self.store.open()

    def load_data(self, key=None, **kwargs):
        """ Lee de disco la información y devuelve"""
        if key is None:
            key = 'data'
        assert (self.store.is_open)
        return pd.read_hdf(self.PATH_DATABASE, key,
                           **kwargs)  # ,where=['index > 2009','index < 2010'],columns=['ordinal']

    def procesa_tareas_paralelo(self, func_process, dict_data, lista_tareas, titulo=None, max_threads=100):
        """Procesa las tareas diarias en paralelo, con un MAX de nº de threads"""
        tic_init = time.time()
        num_tareas = len(lista_tareas)
        if titulo is not None:
            print titulo % num_tareas
        if num_tareas > 1 and self.USAR_MULTITHREAD:
            print u'Comienza la ejecución en paralelo de %lu tareas' % num_tareas
            threads = [threading.Thread(target=func_process, args=(tarea, dict_data,)) for tarea in lista_tareas]
            lista_threads = [threads[i:i + max_threads] for i in np.arange(0, len(threads), max_threads)]
            cont_tareas = 0
            for th in lista_threads:
                tic = time.time()
                [thread.start() for thread in th]
                [thread.join() for thread in th]
                print u"Procesado de tareas en paralelo [%lu->%lu, %%=%.1f]: %.2f seg [%.4f seg/tarea]" \
                      % (cont_tareas + 1, cont_tareas + len(th), 100. * (cont_tareas + len(th)) / float(num_tareas),
                         (time.time() - tic), (time.time() - tic) / len(th))
                cont_tareas += len(th)
        else:
            for tarea in lista_tareas:
                print 'Tarea uni-thread: %s' % str(tarea)
                try:
                    func_process(tarea, dict_data)
                except:
                    print 'ERROR!!!', tarea, lista_tareas
                    print dict_data[tarea]

        tic_fin = (time.time() - tic_init)
        print u"Tiempo de procesado de tareas en paralelo total (%lu tareas): %.2f seg [%.4f seg/tarea]" % (
            num_tareas, tic_fin, tic_fin / num_tareas)

    def append_delta_index(self, ts_data=None, data=None):
        if data is None:
            data = self.data
            reasign = True
        else:
            reasign = False
        print data
        idx_utc = data.index.tz_convert('UTC')
        tt = idx_utc.values
        delta = tt[1:] - tt[:-1]
        if ts_data is None:
            delta /= 1e9  # pasa a segs
            data['delta'] = pd.Series(data=delta, index=data.index[1:])
            data.delta.fillna(1, inplace=True)
        else:
            delta.dtype = np.int64
            delta /= 1e9 * ts_data
            data['delta_T'] = pd.Series(data=delta, index=data.index[1:])
            data.delta_T.fillna(1, inplace=True)
        if reasign:
            self.data = data

    def close(self):
        self.store.close()


# TODO Recolocar?
def corrige_tz_si_procede(df0, tz, inplace=True):
    if tz is not None and df0.index.tzinfo.zone != tz.zone:
        print('Cambiando TZ en merge dataframes: TZ_ini: --> TZ_fin:')
        print 'ANTES:', df0.index
        if inplace:
            df0.set_index(df0.index.tz_convert(tz), inplace=inplace)
            print 'AHORA:', df0.index
        else:
            data = df0.set_index(df0.index.tz_convert(tz), inplace=inplace)
            print 'AHORA:', data.index
            return data


# TODO LIMPIAR MERGE
def merge_lista_df(lista_dfs):
    try:
        df0 = lista_dfs[0]
        # assert(df0.index.tz.zone == 'Europe/Madrid')
        for df1 in lista_dfs[1:]:
            # assert(df1.index.tz.zone == 'Europe/Madrid')
            df0 = df0.merge(df1, how='outer', on=list(df0.columns), left_index=True, right_index=True)
            # assert()
        return df0
    except:
        print 'ERROR MERGE'
        # try:
        #     df0.to_hdf('deb0e.h5', 'table', append=False)
        #     df1.to_hdf('deb1e.h5', 'table', append=False)
        #     df0_bkp.to_hdf('deb1be.h5', 'table', append=False)
        #     print df1.index.tolist()
        #     print df0_bkp.index.tolist()
        #     print df0, df1
        # except:
        #     print 'ERROR CON DF''s?'
        # assert()
        # noinspection PyUnboundLocalVariable
        return df0


#############################
# Main
#############################
def main():
    datos_web = DataWeb('path_data.hf5', u'Datos de web: ____')  # ,
    datos_web.close()
    return datos_web, datos_web.data


if __name__ == '__main__':
    print 'Se devuelven variables: datos_web, data'
    datos_web, data = main()
