# -*- coding: utf-8 -*-
"""
Gestión de datos de demanda energética proporcionados en la web de REE.es
@author: Eugenio Panadero
"""
__author__ = 'Eugenio Panadero'
__copyright__ = "Copyright 2015, AzogueLabs"
__credits__ = ["Eugenio Panadero"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Eugenio Panadero"

import matplotlib.pyplot as plt
# %matplotlib inline
from reedemconf import *

#############################
# Funciones comunes
#############################
FIGSIZE = (12, 6)
DPI = 225
MARGIN_AXIS = [0, 0, 1, 1]


def get_lienzo():
    fig = plt.figure(figsize=FIGSIZE, dpi=DPI)
    hejes = plt.axes(MARGIN_AXIS)
    plt.hold(True)
    return fig, hejes


#############################
# PLOTS
#############################

def plot_prod_vs_dem(prodtot, dem):
    # Plot Producción total vs Demanda
    fig, hejes = get_lienzo()
    plt.plot(prodtot.index, prodtot, label=u'Producción', lw=2.5)
    plt.plot(dem.index, dem, label=u'Demanda', lw=2)
    plt.ylabel('Potencia peninsular (MW)')
    plt.legend()


def plot_ajuste_prod_dem(prodtot, exceso):
    # Plot Ajuste entre Producción total y Demanda
    fig, hejes = get_lienzo()
    plt.plot(prodtot.index, exceso, label=u'Producción - Demanda', lw=2.5)
    plt.ylabel('Potencia peninsular (MW)')
    plt.legend()


def plot_tipos_prod(data, cols_prod=COLS_PROD):
    # Plot Producción por tipos
    fig, hejes = get_lienzo()
    for term in cols_prod:
        plt.plot(data.index, data[term], label=TIPOS_ENER[term], color=COLORES_ENER[term], lw=2)
    plt.ylabel('Potencia peninsular (MW)')
    plt.legend()


def plotarea_stack_tipos_prod(data, cols_prod=COLS_PROD):
    # Plot Stack Producción por tipos
    fig, hejes = get_lienzo()
    base = np.zeros(len(data))
    for term in cols_prod:
        print term, np.mean(base)
        if data[term].any() != 0:
            yacum = data[term] + base
            plt.fill_between(data.index, y1=yacum, y2=base, color=COLORES_ENER[term], lw=0,
                             alpha=.8)  # (x, y1, y2=0, where=None, **kwargs)
            plt.plot(data.index, yacum, label=TIPOS_ENER[term], color=COLORES_ENER[term], lw=2)
            base = yacum
    plt.ylabel('Potencia peninsular (MW)')
    hejes.grid(axis='y')
    plt.legend()


def plot_dia_raro(data, str_dia='2007-11-24'):
    # Plot día raro (datos incorrectos, desconexiones, etc.)
    dia_raro = data[str_dia]
    fig, hejes = get_lienzo()
    dia_raro.plot(ax=hejes)
    plt.title(str_dia)


# dias_raros = ['2007-11-24','2008-01-01','2009-10-06','2011-01-07']
# for d in dias_raros:
#     plot_dia_raro(data, d)


def plot_renov(data):
    # Plot renovables (sin eólica)
    fig, hejes = get_lienzo()
    # data.dem.plot(ax=hejes,kind='Area'), plt.show()
    data.sol.plot(ax=hejes, color=COLORES_ENER['sol'], alpha=.7)
    data.solFot.plot(ax=hejes, color=COLORES_ENER['solFot'], alpha=.7)
    data.solTer.plot(ax=hejes, color=COLORES_ENER['solTer'], alpha=.7)
    data.termRenov.plot(ax=hejes, color=COLORES_ENER['termRenov'], alpha=.5)
    plt.show()


def plot_resample(data, freq='1M', how=np.mean, kind='line', tupla_mag=None):
    # Plot de resample datos'
    # how=[np.min, np.max, np.mean]
    data_rs = data.resample(freq, how=how)
    print data_rs
    fig, hejes = get_lienzo()
    if tupla_mag is not None:
        data_rs.plot(ax=hejes, kind=kind, label=tupla_mag[0], color=tupla_mag[1])
    else:
        data_rs.plot(ax=hejes, kind=kind)
    return data_rs

# data.nuc.plot()


# df0 = get_demanda_ree_en_intervalo(str_dia,str_dia)
# print df0.columns
# print df0.ix[0]
# str_dia = '2007-10-26'

# prod = df0[cols_prod]
# prodtot = prod.sum(axis=1)
# exceso = prod.sum(axis=1) - df0.dem
# #print prodtot.head(), prodtot.tail() #print df0.dem.head(), df0.dem.tail()
# print exceso.head(), exceso.tail()

# cols = list(df0.columns)
# cols.remove('dem')
# print cols

# fig, hejes = get_lienzo()
# df0.plot(y='dem',ax=hejes)
# prod_menos_dem = df0.dem
# df1.plot(y=cols,ax=hejes)
# df2.plot(y=cols,ax=hejes)

# data = pd.DataFrame([df0,df1,df2])


# fig, hejes = get_lienzo()
# #data.dem.plot(ax=hejes,kind='Area'), plt.show()
# data.gf.plot(ax=hejes,label='Fuel',alpha=.7)
# data.hid.plot(ax=hejes,label=u'Hidráulica',alpha=.7)
# data.car.plot(ax=hejes,label=u'Carbón',alpha=.7)
# plt.show()
#
#
# fig, hejes = get_lienzo()
# data.inter.plot(ax=hejes)

#
# print data.ix['2007-10-26 21:00']
# #data.to_html(buf=None, columns=None, col_space=None, colSpace=None, header=True, index=True, na_rep='NaN', formatters=None, float_format=None, sparsify=None, index_names=True, justify=None, bold_rows=True, classes=None, escape=True, max_rows=None, max_cols=None, show_dimensions=False, notebook=False)
# data.to_html('data_ree.html')
