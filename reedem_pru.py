# -*- coding: utf-8 -*-
"""
Gestión de datos de demanda energética proporcionados en la web de REE.es
@author: Eugenio Panadero
"""
import datetime as dt

import numpy as np
import pandas as pd

from reedem import DatosREE
from reedemconf import *
import reedemplot as rdp

# import matplotlib.pyplot as plt
# %matplotlib inline

#############################
# Main
#############################
DATA = DatosREE()
df0 = DATA.data
data = df0.copy()

dfcambio = DATA.get_data_en_intervalo('2013-3-31', '2013-3-31')
DATA.info_data(dfcambio)
dfcambio.plot()
# print df0.dem['2007-10-28 1:00':'2007-10-28 4:00']
# print pd.Timestamp('2007-10-30').weekday()


str_dia = '2007-10-26'
print df0.columns
print df0.ix[0]
prod = df0[COLS_PROD]
prodtot = prod.sum(axis=1)
exceso = prod.sum(axis=1) - df0.dem
# print prodtot.head(), prodtot.tail() #print df0.dem.head(), df0.dem.tail()
print exceso.head(), exceso.tail()


# Plot Producción total vs Demanda
rdp.plot_prod_vs_dem(prodtot, df0.dem)

# Plot Ajuste entre Producción total y Demanda
rdp.plot_ajuste_prod_dem(prodtot, exceso)

# Plot Producción por tipos
rdp.plot_tipos_prod(df0, cols_prod=COLS_PROD)

# Plot Stack Producción por tipos
rdp.plotarea_stack_tipos_prod(df0, cols_prod=COLS_PROD)

idx_utc = data.index.tz_convert('UTC')
tt = idx_utc.values
delta = tt[1:] - tt[:-1]
delta.dtype = np.int64
delta /= 1e9 * TS_DATA
data['delta_T'] = pd.Series(data=delta, index=data.index[1:])
data.delta_T.fillna(1, inplace=True)

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
                       index=data.index[idx_desconex])

errores[errores.anyo != 2009].hist()
errores_anyo = errores[dt.date.today().strftime('%Y')]
print 'Hay %lu errores en la base de datos, producidos en %lu días (%lu de este año).\nEl último fue en %s, con un delta_Ts de %lu' \
      % (len(errores), len(set(errores.ordinal)), len(errores_anyo), errores.index[-1], errores.delta_T[-1])

dias_errores = sorted(set(errores.str_dia))

print data.nuc[data.nuc < 3000]
data.nuc.plot()

print data.car[data.car < 10]
print data['2007-11-24 10:00':'2007-11-24 14:00']


# Plot renovables (sin eólica)
rdp.plot_renov(data)

# Plot hid, car, gf (buscando problemas en datos)
fig, hejes = rdp.get_lienzo()
# data.dem.plot(ax=hejes,kind='Area'), plt.show()
data.gf.plot(ax=hejes, label='Fuel', alpha=.7)
data.hid.plot(ax=hejes, label=u'Hidráulica', alpha=.7)
data.car.plot(ax=hejes, label=u'Carbón', alpha=.7)
rdp.plt.show()

# Días raros (errores en los datos de REE)
for d in DIAS_RAROS:
    rdp.plot_dia_raro(data, d)


# print df0.ix['2007-10-26 21:00']
# fig, hejes = rdp.get_lienzo()
# data.inter.plot(ax=hejes)

#
# #data.to_html(buf=None, columns=None, col_space=None, colSpace=None, header=True, index=True, na_rep='NaN', formatters=None, float_format=None, sparsify=None, index_names=True, justify=None, bold_rows=True, classes=None, escape=True, max_rows=None, max_cols=None, show_dimensions=False, notebook=False)
# data.to_html('data_ree.html')


# Plot resample
intercambios_dia = rdp.plot_resample(data.inter, freq='1M', how=[np.min, np.max, np.mean],
                                     kind='line')  # ,tupla_mag=None)

cols = list(df0.columns)
cols.remove('dem')
print cols

# noinspection PyRedeclaration
fig, hejes = rdp.get_lienzo()
df0.plot(y='dem', ax=hejes)
# prod_menos_dem = df0.dem
# df1.plot(y=cols,ax=hejes)
# df2.plot(y=cols,ax=hejes)

# data = pd.DataFrame([df0,df1,df2])
