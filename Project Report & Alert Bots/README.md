# Report & Alert Bots Project

<div>
  <img src="https://upload.wikimedia.org/wikipedia/commons/d/de/AirflowLogo.png" title="Airflow" alt="Airflow" height="40"/>&nbsp;
  <img src="https://github.com/apache/superset/blob/master/superset-frontend/src/assets/images/clickhouse.png" title="ClickHouse" alt="ClickHouse" height="40"/>
  <img src="https://github.com/devicons/devicon/blob/master/icons/python/python-original-wordmark.svg" title="Python" alt="Python" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/master/icons/pandas/pandas-original-wordmark.svg" title="Pandas" alt="Pandas" width="40" height="40"/>&nbsp;
  <img src="https://user-images.githubusercontent.com/315810/92159303-30d41100-edfb-11ea-8107-1c5352202571.png" title="Seaborn" alt="Seaborn" width="40" height="40"/>&nbsp;
  <img src="https://upload.wikimedia.org/wikipedia/commons/8/84/Matplotlib_icon.svg" title="Matplotlib" alt="Matplotlib" width="40" height="40"/>&nbsp;
  <img src="https://user-images.githubusercontent.com/67586773/105040771-43887300-5a88-11eb-9f01-bee100b9ef22.png" title="NumPy" alt="NumPy" width="40" height="40"/>
</div>

<p></p>

### Info:
Automation of news feed and messenger reporting and creating a Telegram bot.

### Data:
This dataset contains information about users and their interactions with the news feed and messenger.

### Goal:
Create a Telegram bot that will send a message to the chat with a daily report and alerts when anomalies are detected.

### Steps to achieve the goal:
- Сreating a Telegram bot
- Reporting automation
- Metrics Analysis
- Anomaly detection
  - Comparing the value with the value on the previous day at the same time
  - Interquartile range
  - 3-sigma rule

### Tasks:
#### 1. Reports  
   Implement daily sending of a message with a report for yesterday, which contains:
   - Key metrics and their changes compared to the previous day and previous week
   - User activity graphs for the last 7 days
   - Key metrics graphs for the last 7 days
   - Graphs of user activity and key metrics during yesterday and comparison with the previous day and the previous week

#### 2. Alerts  
   Implement sending an alert when anomalies are detected.
   Use the following methods to detect anomalies:
   - Compare the value with the value on the previous day at the same time
   - Interquartile range
   - 3-sigma rule

The decision on the presence of an anomaly is made based on the principle of majority vote. If an anomaly is detected, provide:
   - Metric with an anomalous value
   - Current value
   - Difference with the previous value
   - Difference with the value on the previous day at the same time
   - Graph of metric changes over the last 24 hours and comparison with the previous one
   - Graph of metric changes and interquartile range
   - Graph of metric changes and 3-sigma rule

Check every 15 minutes.

### Result:
#### 1. Reports
Screenshot of the message:
<p align="center">
  <img src="/Project Report & Alert Bots/Images/Report Screen.JPG" height="500"/>
</p>

Graphs in message:
<p align="center">
  <img src="/Project Report & Alert Bots/Images/User Activity – last 7 days.JPG" height="500"/>
  <img src="/Project Report & Alert Bots/Images/Key Metrics – last 7 days.JPG" height="500"/>
  <img src="/Project Report & Alert Bots/Images/User Activity Over Day Comparison.JPG" height="500"/>
</p>

#### 2. Alerts
Screenshot of the message:
<p align="center">
  <img src="/Project Report & Alert Bots/Images/Alert Screen.JPG" height="500"/>
</p>

Graphs in message if anomalies are detected in Active Users Of Messenger and Messages:
<p align="center">
  <img src="/Project Report & Alert Bots/Images/Alarm - Active Users Of Messenger.JPG" height="500"/>
  <img src="/Project Report & Alert Bots/Images/Alarm - Messages.JPG" height="500"/>
</p>
