# Project Delivery App

<div>
  <img src="https://github.com/devicons/devicon/blob/master/icons/jupyter/jupyter-original-wordmark.svg" title="Jupyter" alt="Jupyter" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/master/icons/python/python-original-wordmark.svg" title="Python" alt="Python" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/master/icons/pandas/pandas-original-wordmark.svg" title="Pandas" alt="Pandas" width="40" height="40"/>&nbsp;
  <img src="https://user-images.githubusercontent.com/67586773/105040771-43887300-5a88-11eb-9f01-bee100b9ef22.png" title="NumPy" alt="NumPy" width="40" height="40"/>&nbsp;
  <img src="https://upload.wikimedia.org/wikipedia/commons/b/b2/SCIPY_2.svg" title="SciPy" alt="SciPy" width="40" height="40"/>&nbsp;
  <img src="https://user-images.githubusercontent.com/315810/92159303-30d41100-edfb-11ea-8107-1c5352202571.png" title="Seaborn" alt="Seaborn" width="40" height="40"/>&nbsp;
  <img src="https://upload.wikimedia.org/wikipedia/commons/8/84/Matplotlib_icon.svg" title="Matplotlib" alt="Matplotlib" width="40" height="40"/>
</div>

<p align="right"><i>Project from the Data Analyst course by Karpov.Courses</i></p>

### Info:
The team has introduced a smart product recommendation system into the application. It is expected that such a system will help users work more efficiently with the application and better find the necessary products. An AB test was conducted to test the effectiveness of the recommendation system. First group included users with a new recommendation system, second group included users with an old version of the application, which does not recommend products.

### Data:  
- user order history, information about which orders were created and canceled by users
- detailed information about the contents of the order, list of products ids for each order
- detailed information about products: names and prices

### Goal:
Analytical conclusion whether to include a new recommendation system for all users.

### Steps to achieve the goal:
- Pre-analysis
- Exploratory Data Analysis
- A/B Testing Result Analysis
- Selecting metrics
- Hypothesis testing
- Calculating statistical significance
- Student's t-test, Welch's t-test, Chi-squared test

### Tasks:
Select service quality metrics and statistically compare them in two groups.

### Results:
Statistical tests were performed for the following metrics:
- Cancellation Rate
- Average Order Value
- Average Order Size
- Average Revenue per User
- Purchase Frequency
- Repeat Purchase Rate

It turned out that Cancellation Rate, AOV, AOS did not change significantly. However, at the same time, ARPU, PF, and RPR increased statistically significantly. That is, users began to make more orders, make repeat purchases more often, and bring more income to the business. Thus, with the introduction of the recommendation system, the business began to generate more income, and users began to better satisfy their needs.  
**Conclusion:** the new recommendation system was able to benefit the business and app users.




