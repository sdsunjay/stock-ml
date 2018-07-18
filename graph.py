# THIS VERSION IS FOR PYTHON 3 #
import sys, traceback
# import urllib.request, urllib.error, urllib.parse
import time
import datetime
import numpy as np
from numpy import array
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates
from mpl_finance import candlestick_ohlc
# from mpl-finance import candlestick_ohlc
import matplotlib
import pylab
matplotlib.rcParams.update({'font.size': 9})

def rsiFunc(prices, n=14):
    deltas = np.diff(prices)
    seed = deltas[:n+1]
    up = seed[seed>=0].sum()/n
    down = -seed[seed<0].sum()/n
    rs = up/down
    rsi = np.zeros_like(prices)
    rsi[:n] = 100. - 100./(1.+rs)

    for i in range(n, len(prices)):
        delta = deltas[i-1] # cause the diff is 1 shorter

        if delta>0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta

        up = (up*(n-1) + upval)/n
        down = (down*(n-1) + downval)/n

        rs = up/down
        rsi[i] = 100. - 100./(1.+rs)

    return rsi

def bytespdate2num(fmt, encoding='utf-8'):
    strconverter = mdates.strpdate2num(fmt)
    def bytesconverter(b):
        s = b.decode(encoding)
        return strconverter(s)
    return bytesconverter


def calculate_sma(values, window):
    sum1 = 0
    for i in range(window):
        if float(values[i]) == 0.0:
            continue
        sum1+= float(values[i])
    return float("{0:.2f}".format(sum1/window))

def movingaverage_sunjay(values,window):
    # print(values)
    # print(window)
    smas = []
    weights = np.repeat(1.0, window)/window
    print(len(values))
    for i in range(len(values)):
        if i+window > len(values):
            break
        sma = calculate_sma(values[i:i+window], window)
        smas.append(sma)
        i+=1
    # smas = np.convolve(values, weights, 'valid')
    # return smas # as a numpy array
    return array(smas)

def movingaverage(values,window):
    weigths = np.repeat(1.0, window)/window
    smas = np.convolve(values, weigths, 'valid')
    return smas # as a numpy array

def ExpMovingAverage(values, window):
    weights = np.exp(np.linspace(-1., 0., window))
    weights /= weights.sum()
    print(str(window))
    # print(str(values))
    a =  np.convolve(values, weights, mode='full')[:len(values)]
    a[:window] = a[window]
    return a


def computeMACD(x, slow=26, fast=12):
    """
    compute the MACD (Moving Average Convergence/Divergence) using a fast and slow exponential moving avg'
    return value is emaslow, emafast, macd which are len(x) arrays
    """
    emaslow = ExpMovingAverage(list(map(float,x)),slow)
    emafast = ExpMovingAverage(list(map(float,x)),fast)
    return emaslow, emafast, emafast - emaslow

def fetch_data_from_file(filename):
    data = []
    with open(filename) as f:
        # read first line to remove header
        f.readline()
        for line in f:
            data.append(line.strip().split(','))
    return data

def column(matrix, i):
    return [row[i] for row in matrix]

def graphData(stock,MA1,MA2):

    '''
        Use this to dynamically pull a stock:
    '''
   # try:
   #     print('Currently Pulling',stock)
   #     urlToVisit = 'http://chartapi.finance.yahoo.com/instrument/1.0/'+stock+'/chartdata;type=quote;range=10y/csv'
   #     stockFile =[]
   #     try:
   #         sourceCode = urllib.request.urlopen(urlToVisit).read().decode()
   #         splitSource = sourceCode.split('\n')
   #         for eachLine in splitSource:
   #             splitLine = eachLine.split(',')
   #             if len(splitLine)==6:
   #                 if 'values' not in eachLine:
   #                     stockFile.append(eachLine)
   #     except Exception as e:
   #         print(str(e), 'failed to organize pulled data.')
   # except Exception as e:
   #     print(str(e), 'failed to pull pricing data')
    stockData = fetch_data_from_file('data/raw/'+stock+'.csv')
    date = column(stockData, 1)
    closep = column(stockData, 2)
    highp = column(stockData, 3)
    lowp = column(stockData, 4)
    openp = column(stockData, 5)
    volume = column(stockData, 6)
    # Symbol,Date,Close,High,Low,Open,Volume
    try:
        # date, closep, highp, lowp, openp, volume = np.loadtxt(stockFile,delimiter=',', unpack=True, converters={ 0: bytespdate2num('%Y%m%d')})
        x = 0
        y = len(date)
        newAr = []
        while x < y:
            appendLine = date[x],openp[x],highp[x],lowp[x],closep[x],volume[x]
            newAr.append(appendLine)
            x+=1

        Av1 = movingaverage(list(map(float,closep)), MA1)
        Av2 = movingaverage(list(map(float,closep)), MA2)
        SP = len(date[MA2-1:])
        SP = int(SP)
        fig = plt.figure(facecolor='#07000d')

        # ax1 = plt.subplot2grid((6,4), (1,0), rowspan=4, colspan=4, axisbg='#07000d')
        ax1 = plt.subplot2grid((6,4), (1,0), rowspan=4, colspan=4)
        # candlestick_ohlc(ax1, newAr[-SP:], width=0.6, colorup='#53c156', colordown='#ff1717')

        Label1 = str(MA1)+' SMA'
        Label2 = str(MA2)+' SMA'

        ax1.plot(date[-SP:],Av1[-SP:],'#e1edf9',label=Label1, linewidth=1.5)
        ax1.plot(date[-SP:],Av2[-SP:],'#4ee6fd',label=Label2, linewidth=1.5)

        ax1.grid(True, color='w')
        ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax1.yaxis.label.set_color("w")
        ax1.spines['bottom'].set_color("#5998ff")
        ax1.spines['top'].set_color("#5998ff")
        ax1.spines['left'].set_color("#5998ff")
        ax1.spines['right'].set_color("#5998ff")
        ax1.tick_params(axis='y', colors='w')
        plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
        ax1.tick_params(axis='x', colors='w')
        plt.ylabel('Stock price and Volume')

        maLeg = plt.legend(loc=9, ncol=2, prop={'size':7}, fancybox=True, borderaxespad=0.)
        maLeg.get_frame().set_alpha(0.4)
        textEd = pylab.gca().get_legend().get_texts()
        pylab.setp(textEd[0:5], color = 'w')

        volumeMin = 0

        # ax0 = plt.subplot2grid((6,4), (0,0), sharex=ax1, rowspan=1, colspan=4, axisbg='#07000d')
        ax0 = plt.subplot2grid((6,4), (0,0), sharex=ax1, rowspan=1, colspan=4)
        rsi = rsiFunc(list(map(float,closep)))
        print(str(rsi))
        rsiCol = '#c1f9f7'
        posCol = '#386d13'
        negCol = '#8f2020'

        ax0.plot(date[-SP:], rsi[-SP:], rsiCol, linewidth=1.5)
        ax0.axhline(70, color=negCol)
        ax0.axhline(30, color=posCol)
        ax0.fill_between(date[-SP:], rsi[-SP:], 70, where=(rsi[-SP:]>=70), facecolor=negCol, edgecolor=negCol, alpha=0.5)
        ax0.fill_between(date[-SP:], rsi[-SP:], 30, where=(rsi[-SP:]<=30), facecolor=posCol, edgecolor=posCol, alpha=0.5)
        ax0.set_yticks([30,70])
        ax0.yaxis.label.set_color("w")
        ax0.spines['bottom'].set_color("#5998ff")
        ax0.spines['top'].set_color("#5998ff")
        ax0.spines['left'].set_color("#5998ff")
        ax0.spines['right'].set_color("#5998ff")
        ax0.tick_params(axis='y', colors='w')
        ax0.tick_params(axis='x', colors='w')
        plt.ylabel('RSI')

        ax1v = ax1.twinx()
        ax1v.fill_between(date[-SP:],volumeMin, volume[-SP:], facecolor='#00ffe8', alpha=.4)
        ax1v.axes.yaxis.set_ticklabels([])
        ax1v.grid(False)
        ###Edit this to 3, so it's a bit larger
        volume_max = 3 * float(max(volume))
        ax1v.set_ylim(0, volume_max)
        ax1v.spines['bottom'].set_color("#5998ff")
        ax1v.spines['top'].set_color("#5998ff")
        ax1v.spines['left'].set_color("#5998ff")
        ax1v.spines['right'].set_color("#5998ff")
        ax1v.tick_params(axis='x', colors='w')
        ax1v.tick_params(axis='y', colors='w')
        # ax2 = plt.subplot2grid((6,4), (5,0), sharex=ax1, rowspan=1, colspan=4, axisbg='#07000d')
        ax2 = plt.subplot2grid((6,4), (5,0), sharex=ax1, rowspan=1, colspan=4)
        fillcolor = '#00ffe8'
        nslow = 26
        nfast = 12
        nema = 9
        emaslow, emafast, macd = computeMACD(closep)
        ema9 = ExpMovingAverage(list(map(float,macd)),nema)

        ax2.plot(date[-SP:], macd[-SP:], color='#4ee6fd', lw=2)
        ax2.plot(date[-SP:], ema9[-SP:], color='#e1edf9', lw=1)
        ax2.fill_between(date[-SP:], macd[-SP:]-ema9[-SP:], 0, alpha=0.5, facecolor=fillcolor, edgecolor=fillcolor)

        plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
        ax2.spines['bottom'].set_color("#5998ff")
        ax2.spines['top'].set_color("#5998ff")
        ax2.spines['left'].set_color("#5998ff")
        ax2.spines['right'].set_color("#5998ff")
        ax2.tick_params(axis='x', colors='w')
        ax2.tick_params(axis='y', colors='w')
        plt.ylabel('MACD', color='w')
        ax2.yaxis.set_major_locator(mticker.MaxNLocator(nbins=5, prune='upper'))
        for label in ax2.xaxis.get_ticklabels():
            label.set_rotation(45)

        plt.suptitle(stock.upper(),color='w')
        plt.setp(ax0.get_xticklabels(), visible=False)
        plt.setp(ax1.get_xticklabels(), visible=False)

        ax1.annotate('Big news!',(date[510],Av1[510]),
            xytext=(0.8, 0.9), textcoords='axes fraction',
            arrowprops=dict(facecolor='white', shrink=0.05),
            fontsize=14, color = 'w',
            horizontalalignment='right', verticalalignment='bottom')

        plt.subplots_adjust(left=.09, bottom=.14, right=.94, top=.95, wspace=.20, hspace=0)
        plt.show()
        fig.savefig('example.png',facecolor=fig.get_facecolor())

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("*** print_tb:")
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        print("*** print_exception:")
        # exc_type below is ignored on 3.5 and later
        traceback.print_exception(exc_type, exc_value,
                exc_traceback, limit=2, file=sys.stdout)
        print("*** print_exc:")
        traceback.print_exc(limit=2, file=sys.stdout)
        print("*** format_exc, first and last line:")
        formatted_lines = traceback.format_exc().splitlines()
        print(formatted_lines[0])
        print(formatted_lines[-1])
        print("*** format_exception:")
        # exc_type below is ignored on 3.5 and later
        print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
        print("*** extract_tb:")
        print(repr(traceback.extract_tb(exc_traceback)))
        print("*** format_tb:")
        print(repr(traceback.format_tb(exc_traceback)))
        print("*** tb_lineno:", exc_traceback.tb_lineno)
# while True:
    # stock = input('Stock to plot: ')
stock = 'BABA'
graphData(stock,10,50)
