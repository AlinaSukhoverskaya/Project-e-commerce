# Подгружаем библиотеки
import pandas as pd
import pandahouse as ph
import numpy as np
import telegram
import io
from datetime import datetime, date, timedelta, time

import matplotlib.pyplot as plt
import matplotlib.ticker
import seaborn as sns

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Параметры соединения, чтобы подключиться к схеме данных simulator_20231220
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'simulator_20231220',
    'user':'student',
    'password':'dpo_python_2020'
}

# Параметры бота
my_chat_id = 403727131
my_token = '6989711871:AAFLsOG8pEqMLs_qJj4Dsg2pSwhZF0YjtfE'
bot = telegram.Bot(token=my_token)

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a.sukhoverskaya',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 20),
}

schedule_interval = '*/15 * * * *'


# Функция c алгоритмом проверки значения на аномальность посредством сравнения интересующего значения со значением в это же время сутки назад
def previous_comparison(df, metric, threshold=0.3):
    current_ts = df['Time'].max()
    day_ago_ts = current_ts - pd.DateOffset(days=1)
    
    current_value = df[df['Time'] == current_ts][metric].iloc[-1]
    day_ago_value = df[df['Time'] == day_ago_ts][metric].iloc[-1]

    if current_value <= day_ago_value:
        diff = abs(current_value / day_ago_value - 1)
    else:
        diff = abs(day_ago_value / current_value - 1)

    if diff > threshold:
        is_alert = 1
    else:
        is_alert = 0
        
    return is_alert, current_value, day_ago_value

# Функция c алгоритмом проверки значения на аномальность с помощью правила трех сигм
def three_sigma(df, metric, a=3, n=5):
    before_yesterday = df['Date'].min()
    df = df.copy().query('Date > @before_yesterday')
    mean = df[metric].rolling(n).mean()
    var = df[metric].rolling(n).std()
    df['up'] = mean + a * var
    df['low'] = mean - a * var
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
        
    return is_alert, df

# Функция c алгоритмом проверки значения на аномальность с помощью межквартильного размаха
def iqr(df, metric, a=4, n=3):
    before_yesterday = df['Date'].min()
    df = df.copy().query('Date > @before_yesterday')
    df['q_25'] = df[metric].shift().rolling(n).quantile(0.25)
    df['q_75'] = df[metric].shift().rolling(n).quantile(0.75)
    df['iqr'] = df['q_75'] - df['q_25']
    df['up'] = df['q_75'] + a * df['iqr']
    df['low'] = df['q_25'] - a * df['iqr']
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0 
        
    return is_alert, df

# Получаем графики
def get_graphs(data, df_iqr, df_sigma, metric):
    sns.set_style('whitegrid')
    fig, axes = plt.subplots(3, 1, figsize=(12, 18))
    plt.subplots_adjust(hspace=0.4, bottom=0.08, top=0.92, right=0.95, left=0.06)
    fig.suptitle('ALARM! ' + metric + '!', fontsize=24, fontweight='bold', y=0.98)
    
    today_end = data['Time'].max()    
    today_start = today_end - pd.DateOffset(days=1)
    today_data = data.query('@today_start <= Time <= @today_end')

    yesterday_end = today_start
    yesterday_start = yesterday_end - pd.DateOffset(days=1)
    yesterday_data = data.query('@yesterday_start <= Time <= @yesterday_end')

    axes[0].plot(yesterday_data.Time + timedelta(days = 1), yesterday_data[metric], 
                 label='1 day offset', linewidth=2, color='#a0a0a0', linestyle='--')
    axes[0].plot(today_data.Time, today_data[metric], label='Last 24 hours', linewidth=2, color='#EC7C26')
    axes[0].set_title('Comparison with the previous day', fontsize=20)
    
    axes[1].plot(df_iqr.Time, df_iqr[metric], label='Actual', linewidth=2, color='#EC7C26')
    axes[1].plot(df_iqr.Time, df_iqr.up, label='Upper bound', linewidth=2, color='#baada5')
    axes[1].plot(df_iqr.Time, df_iqr.low, label='Lower bound', linewidth=2, color='#B79479')
    axes[1].set_title('Interquartile range', fontsize=20)
    
    axes[2].plot(df_iqr.Time, df_sigma[metric], label='Actual', linewidth=2, color='#EC7C26')
    axes[2].plot(df_iqr.Time, df_sigma.up, label='Upper bound', linewidth=2, color='#baada5')
    axes[2].plot(df_iqr.Time, df_sigma.low, label='Lower bound', linewidth=2, color='#B79479')
    axes[2].set_title('3-sigma rule', fontsize=20)
    
    for i in range(3):
        axes[i].xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
        axes[i].set_ylabel('')
        axes[i].set_xlabel('Time', fontsize=18, labelpad=12)
        axes[i].tick_params(axis='both', which='major', labelsize=14)
        axes[i].legend(fontsize="large")
    
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plt.close()
    
    return plot_object


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_alert():
    
    # Отправка алерта
    @task()
    def run_alerts(chat=None):
        chat_id = chat or my_chat_id

        # Выполняем запрос и записываем его в датафрейм
        query = '''
                SELECT
                    toDate(fifteen_minutes_time) AS Date,
                    fifteen_minutes_time AS Time,
                    countIf(DISTINCT user_id, action = 'like' or action = 'view') AS "Active Users Of News Feed",
                    countIf(DISTINCT user_id, action = 'send_message') AS "Active Users Of Messenger",
                    countIf(action = 'view') AS Views,
                    countIf(action = 'like') AS Likes,
                    multiIf(Views > 0, ROUND(Likes / Views, 3), 0) AS CTR,
                    countIf(action = 'send_message') AS Messages
                FROM
                    (SELECT 
                        toStartOfFifteenMinutes(time) AS fifteen_minutes_time,
                        user_id,
                        action
                    FROM simulator_20231220.feed_actions
                    UNION ALL
                    SELECT 
                        toStartOfFifteenMinutes(time) AS fifteen_minutes_time,
                        user_id,
                        'send_message' AS action
                    FROM simulator_20231220.message_actions) AS t
                WHERE Date >= today() - 2 and Time < toStartOfFifteenMinutes(now())
                GROUP BY Time, Date
                ORDER BY fifteen_minutes_time
                '''
        data = ph.read_clickhouse(query, connection=connection)

        metrics = ['Active Users Of News Feed', 'Views', 'Likes', 'CTR', 'Active Users Of Messenger', 'Messages']

        # Проверяем метрики на аномальность
        for metric in metrics:
            df = data[['Time', 'Date', metric]].copy()

            is_alert_previous, current_value, day_ago_value = previous_comparison(df, metric)
            is_alert_iqr, df_iqr = iqr(df, metric)
            is_alert_sigma, df_sigma = three_sigma(df, metric)

            # Решение об аномальности наблюдения принимаем по принципу большинства голосов
            if is_alert_iqr + is_alert_sigma + is_alert_previous > 1:  
                current_val = df[metric].iloc[-1]
                difference = round(100 * (df[metric].iloc[-1] / df[metric].iloc[-2] - 1), 2)
                difference_prev = round(100 * (current_value / day_ago_value - 1), 2)
                    
                if difference > 0:
                    difference = '+' + str(difference)
                else:
                    difference = str(difference)
                    
                if difference_prev > 0:
                    difference_prev = '+' + str(difference_prev)
                else:
                    difference_prev = str(difference_prev)

                message = '<b>ALARM! Anomaly detected!</b>' + f'''
Time: {pd.to_datetime(data.Time.values[-1]).strftime('%H:%M, %Y-%m-%d')}
Metric: {metric}
Current Value: {current_val}
{difference}% vs previous value 
{difference_prev}% vs previous day value
<a href="http://superset.lab.karpov.courses/r/5013">Link to dashboard</a>
'''
                plot_object = get_graphs(data, df_iqr, df_sigma, metric)
                bot.sendMessage(chat_id=chat_id, text=message, parse_mode='html')
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    run_alerts(my_chat_id)
alert_dag = dag_alert()