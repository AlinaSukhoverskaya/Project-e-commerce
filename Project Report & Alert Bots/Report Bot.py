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
  
schedule_interval = '5 0 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report():
    
    # Выполняем запрос и записываем его в датафрейм, получаем метрики за прошлую неделю по дням
    @task()
    def get_metrics_previous_week():
        metrics_previous_week = '''
                                SELECT
                                    date AS Date,
                                    
                                    count(DISTINCT user_id) AS DAU,
                                    countIf(DISTINCT user_id, is_feed_user = 1 AND is_messenger_user = 1) AS dau_both_products,
                                    countIf(DISTINCT user_id, is_feed_user = 1 AND is_messenger_user = 0) AS dau_feed_only,
                                    countIf(DISTINCT user_id, is_feed_user = 0 AND is_messenger_user = 1) AS dau_messenger_only,
                                    countIf(DISTINCT user_id, is_feed_user = 1) AS dau_feed,
                                    countIf(DISTINCT user_id, is_messenger_user = 1) AS dau_messenger,

                                    countIf(DISTINCT user_id, start_date=date) AS new_users,
                                    countIf(DISTINCT user_id, start_date=date and source='organic') AS organic_new_users,
                                    countIf(DISTINCT user_id, start_date=date and source='ads') AS ads_new_users,

                                    COUNT(user_id) AS actions,
                                    SUM(is_feed) AS actions_feed,
                                    SUM(is_messenger) AS actions_messenger,
                                    
                                    countIf(action = 'view') AS Views,
                                    countIf(action = 'like') AS Likes,
                                    multiIf(Views > 0, ROUND(Likes / Views, 3), 0) AS CTR,
                                    countIf(action = 'send_message') AS Messages
                                FROM
                                    (SELECT
                                        date,
                                        user_id,
                                        action,
                                        source,
                                        is_feed,
                                        is_messenger,
                                        MIN(date) OVER (PARTITION BY user_id, source) AS start_date,
                                        MAX(is_feed) OVER (PARTITION BY user_id, date) AS is_feed_user,
                                        MAX(is_messenger) OVER (PARTITION BY user_id, date) AS is_messenger_user    
                                    FROM
                                        (SELECT 
                                            1 AS is_feed,
                                            0 AS is_messenger,
                                            toDate(time) AS date,
                                            user_id,
                                            action,
                                            source
                                        FROM simulator_20231220.feed_actions
                                        UNION ALL
                                        SELECT 
                                            0 AS is_feed,
                                            1 AS is_messenger,
                                            toDate(time) AS date,
                                            user_id,
                                            'send_message' AS action,
                                            source
                                        FROM simulator_20231220.message_actions)) AS t
                                WHERE date BETWEEN today() - 8 and yesterday()
                                GROUP BY date
                                ORDER BY date DESC
                                '''
        metrics_previous_week = ph.read_clickhouse(metrics_previous_week, connection=connection)
        return metrics_previous_week
    
    # Выполняем запрос и записываем его в датафрейм, получаем метрики за вчера, позавчера и неделю назад с 15-минутным интервалом
    @task()
    def get_metrics_comparison():
        metrics_comparison = '''
                        SELECT
                            toDate(fifteen_minutes_time) AS Date,
                            fifteen_minutes_time AS Time,
                            count(DISTINCT user_id) AS DAU,
                            countIf(DISTINCT user_id, fifteen_minutes_time = start_fifteen_minutes) AS "New Users",
                            countIf(action = 'view') AS Views,
                            countIf(action = 'like') AS Likes,
                            multiIf(Views > 0, ROUND(Likes / Views, 3), 0) AS CTR,
                            countIf(action = 'send_message') AS Messages
                        FROM
                            (SELECT
                                fifteen_minutes_time,
                                user_id,
                                action,
                                MIN(fifteen_minutes_time) OVER (PARTITION BY user_id) AS start_fifteen_minutes
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
                                FROM simulator_20231220.message_actions)) AS t
                        WHERE toDate(fifteen_minutes_time) IN (yesterday(), yesterday() - 1, yesterday() - 7)
                        GROUP BY fifteen_minutes_time
                        ORDER BY fifteen_minutes_time DESC
                        '''
        metrics_comparison = ph.read_clickhouse(metrics_comparison, connection=connection)
        return metrics_comparison
    
    # Пишем сообщение с ежедневным детальным отчетом
    @task()
    def get_message(metrics_previous_week):
        metrics_previous_week['org_perc'] = round(100 * (metrics_previous_week.organic_new_users / metrics_previous_week.new_users).replace(np.inf, 0), 2)
        metrics_previous_week['ads_perc'] = round(100 * (metrics_previous_week.ads_new_users / metrics_previous_week.new_users).replace(np.inf, 0), 2)
        metrics_previous_week['f_only_perc'] = round(100 * (metrics_previous_week.dau_feed_only / metrics_previous_week.DAU).replace(np.inf, 0), 2)
        metrics_previous_week['m_only_perc'] = round(100 * (metrics_previous_week.dau_messenger_only / metrics_previous_week.DAU).replace(np.inf, 0), 2)
        metrics_previous_week['both_perc'] = round(100 * (metrics_previous_week.dau_both_products / metrics_previous_week.DAU).replace(np.inf, 0), 2)
        
        yesterday = date.today() - timedelta(days = 1)
        yesterday_previous_day = date.today() - timedelta(days = 2)
        yesterday_previous_week = date.today() - timedelta(days = 8)

        yesterday_metrics = metrics_previous_week.query("Date == @yesterday")
        yesterday_previous_day_metrics = metrics_previous_week.query("Date == @yesterday_previous_day")
        yesterday_previous_week_metrics = metrics_previous_week.query("Date == @yesterday_previous_week")

        metrics = ['DAU', 'new_users', 'actions', 'Views', 'Likes', 'CTR', 'Messages']
        diff_day = []
        diff_week = []
        for metric in metrics:
            metric_main = yesterday_metrics[metric].values[0]
            metric_previous_day = yesterday_previous_day_metrics[metric].values[0]
            yesterday_previous_week = yesterday_previous_week_metrics[metric].values[0]

            difference_day = round(metric_main * 100 / metric_previous_day - 100, 2)
            difference_week = round(metric_main * 100 / yesterday_previous_week - 100, 2)

            if difference_day > 0:
                difference_day = str('+') + str(difference_day)

            if difference_week > 0:
                difference_week = str('+') + str(difference_week)

            difference_day = str(difference_day) + str('%')
            difference_week = str(difference_week) + str('%')

            diff_day.append(difference_day)
            diff_week.append(difference_week)
            
        message = '<b>Daily Detailed Key Metrics Report</b>' + f'''
Date: {np.datetime_as_string(yesterday_metrics.Date.values[0], unit='D')}

DAU: {yesterday_metrics.DAU.values[0]}
{diff_day[0]} vs previous day
{diff_week[0]} vs previous week
{yesterday_metrics.dau_both_products.values[0]} ({str(yesterday_metrics.both_perc[0])}%) - both products
{yesterday_metrics.dau_feed_only.values[0]} ({str(yesterday_metrics.f_only_perc[0])}%) - only news feed
{yesterday_metrics.dau_messenger_only.values[0]} ({str(yesterday_metrics.m_only_perc[0])}%) - only messenger

New Users: {yesterday_metrics.new_users.values[0]}
{diff_day[1]} vs previous day
{diff_week[1]} vs previous week
{yesterday_metrics.ads_new_users.values[0]} ({str(yesterday_metrics.ads_perc[0])}%) - ads
{yesterday_metrics.organic_new_users.values[0]} ({str(yesterday_metrics.org_perc[0])}%) - organic

Actions: {yesterday_metrics.actions.values[0]}
{diff_day[2]} vs previous day
{diff_week[2]} vs previous week

Views: {yesterday_metrics.Views.values[0]}
{diff_day[3]} vs previous day
{diff_week[3]} vs previous week

Likes: {yesterday_metrics.Likes.values[0]}
{diff_day[4]} vs previous day
{diff_week[4]} vs previous week

CTR: {yesterday_metrics.CTR.values[0]}
{diff_day[5]} vs previous day
{diff_week[5]} vs previous week

Messages: {yesterday_metrics.Messages.values[0]}
{diff_day[6]} vs previous day
{diff_week[6]} vs previous week'''
        return message
    
    # Получаем графики со значениями метрик активности пользователей за предыдущие 7 дней
    @task()
    def get_graphs_activity(metrics_previous_week):
        sns.set_style('whitegrid')
        fig, axes = plt.subplots(4, 1, figsize=(12, 18))
        plt.subplots_adjust(hspace=0.6, bottom=0.08, top=0.92, right=0.97, left=0.08)
        fig.suptitle('User Activity – last 7 days', fontsize=24, fontweight='bold', y=0.98)
        
        metrics_previous_week = metrics_previous_week[:-1]
        axes[0].plot(metrics_previous_week.Date, metrics_previous_week.DAU, label='All active users', linewidth=2, color='#6C6960')
        axes[0].plot(metrics_previous_week.Date, metrics_previous_week.dau_both_products, label='Both products', linewidth=2, color='#CAA885')
        axes[0].plot(metrics_previous_week.Date, metrics_previous_week.dau_feed_only, label='News feed onle', linewidth=2, color='#884535')
        axes[0].plot(metrics_previous_week.Date, metrics_previous_week.dau_messenger_only, label='Messenger only', linewidth=2, color='#EC7C26')
        axes[0].legend(fontsize="large")
        axes[0].set_title('Active Users', fontsize=22)

        axes[1].plot(metrics_previous_week.Date, metrics_previous_week.dau_feed, label='News feed', linewidth=2, color='#884535')
        axes[1].plot(metrics_previous_week.Date, metrics_previous_week.dau_messenger, label='Messenger', linewidth=2, color='#EC7C26')
        axes[1].legend(fontsize="large")
        axes[1].set_title('DAU', fontsize=22)

        axes[2].plot(metrics_previous_week.Date, metrics_previous_week.new_users, linewidth=2, color='#CAA885')
        axes[2].set_title('New Users', fontsize=22)

        axes[3].plot(metrics_previous_week.Date, metrics_previous_week.actions, linewidth=2, color='#6C6960')
        axes[3].set_title('Actions', fontsize=22)

        for i in range(4):
            axes[i].set_xlabel('Date', fontsize=18)
            axes[i].tick_params(labelsize=14)
            axes[i].tick_params(labelsize=14)
            
        graphs_activity = io.BytesIO()
        plt.savefig(graphs_activity)
        graphs_activity.seek(0)
        plt.close()
        return graphs_activity

    # Получаем графики со значениями метрик за предыдущие 7 дней
    @task()
    def get_graphs(metrics_previous_week):
        metrics_name = ['Views', 'Likes', 'CTR', 'Messages']
        colors = ['#EC7C26', '#CAA885', '#6C6960', '#884535']

        sns.set_style('whitegrid')
        fig, axes = plt.subplots(len(metrics_name), 1, figsize=(12, 18))
        plt.subplots_adjust(hspace=0.6, bottom=0.08, top=0.92, right=0.97, left=0.08)
        fig.suptitle('Key Metrics – last 7 days', fontsize=24, fontweight='bold', y=0.98)

        for i in range(len(metrics_name)):
            sns.lineplot(ax=axes[i], data=metrics_previous_week[:-1], 
                         x='Date', y=metrics_name[i],
                         linewidth = 2, color=colors[i],
                         marker='.', markersize=15,
                        )
            axes[-1].yaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter('%.0f'))
            axes[i].set_ylabel('')
            axes[i].set_xlabel('Date', fontsize=18, labelpad=12)
            axes[i].set_title(metrics_name[i], fontsize=22)
            axes[i].tick_params(axis='both', which='major', labelsize=14)

        graphs = io.BytesIO()
        plt.savefig(graphs)
        graphs.seek(0)
        plt.close()
        return graphs
    
    # Получаем графики с сравнением динамики метрик за вчера, позавчера и неделю назад
    @task()
    def get_graphs_comparison(metrics_comparison):
        
        yesterday_end = datetime.combine(date.today(), time(0, 0))
        yesterday_start = yesterday_end - timedelta(days = 1)
        yesterday_metrics = metrics_comparison.query('@yesterday_start <= Time <= @yesterday_end')
        
        yesterday_pr_end = yesterday_start
        yesterday_pr_start = yesterday_pr_end - timedelta(days = 1)
        yesterday_previous_day_metrics = metrics_comparison.query('@yesterday_pr_start <= Time <= @yesterday_pr_end')

        yesterday_pr_w_end = yesterday_start - timedelta(days = 6)
        yesterday_pr_w_start = yesterday_pr_w_end - timedelta(days = 1)
        yesterday_previous_week_metrics = metrics_comparison.query('@yesterday_pr_w_start <= Time <= @yesterday_pr_w_end')

        metrics_name = [['DAU', 'New Users'], ['Views', 'Likes'], ['CTR', 'Messages']]

        fig, axes = plt.subplots(3, 2, figsize=(22, 18))
        plt.subplots_adjust(hspace=0.4, bottom=0.08, top=0.92, right=0.97, left=0.05)
        fig.suptitle('User Activity Over Day Comparison', fontsize=24, fontweight='bold', y=0.98)
        
        for row in range(3):
            for col in range(2):
                axes[row, col].plot(yesterday_metrics.Time, yesterday_metrics[metrics_name[row][col]], 
                                    label=date.today() - timedelta(days = 1), linewidth=3, color='#EC7C26')
                axes[row, col].plot(yesterday_previous_day_metrics.Time + timedelta(days = 1), yesterday_previous_day_metrics[metrics_name[row][col]], 
                                    label='Previous day', linewidth=2, color='#a0a0a0', linestyle='--')
                axes[row, col].plot(yesterday_previous_week_metrics.Time + timedelta(days = 7), yesterday_previous_week_metrics[metrics_name[row][col]], 
                                    label='Previous week', linewidth=2, color='#a0a0a0', linestyle=':')
                axes[row, col].xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
                axes[row, col].set_ylabel('')
                axes[row, col].set_xlabel('Time', fontsize=18, labelpad=12)
                axes[row, col].set_title(metrics_name[row][col], fontsize=22)
                axes[row, col].tick_params(axis='both', which='major', labelsize=14)
                axes[row, col].legend(fontsize="large")

        graphs_comparison = io.BytesIO()
        plt.savefig(graphs_comparison)
        graphs_comparison.seek(0)
        plt.close()
        return graphs_comparison           
         
    # Отправляем отчет
    @task()
    def send_report(message, graphs_activity, graphs, graphs_comparison, chat=None):
        chat_id = chat or my_chat_id
        bot.sendMessage(chat_id=chat_id, text=message, parse_mode='html')
        bot.sendPhoto(chat_id=chat_id, photo=graphs_activity)
        bot.sendPhoto(chat_id=chat_id, photo=graphs)
        bot.sendPhoto(chat_id=chat_id, photo=graphs_comparison)
        
    metrics_previous_week = get_metrics_previous_week()
    metrics_comparison = get_metrics_comparison()
    message = get_message(metrics_previous_week)
    graphs_activity = get_graphs_activity(metrics_previous_week)
    graphs = get_graphs(metrics_previous_week)
    graphs_comparison = get_graphs_comparison(metrics_comparison)
    send_report(message, graphs_activity, graphs, graphs_comparison, my_chat_id)
    
report_dag = dag_report()