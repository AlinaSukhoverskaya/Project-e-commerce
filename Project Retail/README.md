# Project Retail

<div>
  <img src="https://github.com/devicons/devicon/blob/master/icons/jupyter/jupyter-original-wordmark.svg" title="Jupyter" alt="Jupyter" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/master/icons/python/python-original-wordmark.svg" title="Python" alt="Python" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/master/icons/pandas/pandas-original-wordmark.svg" title="Pandas" alt="Pandas" width="40" height="40"/>&nbsp;
  <img src="https://user-images.githubusercontent.com/67586773/105040771-43887300-5a88-11eb-9f01-bee100b9ef22.png" title="NumPy" alt="NumPy" width="40" height="40"/>&nbsp;
  <img src="https://user-images.githubusercontent.com/315810/92159303-30d41100-edfb-11ea-8107-1c5352202571.png" title="Seaborn" alt="Seaborn" width="40" height="40"/>&nbsp;
  <img src="https://upload.wikimedia.org/wikipedia/commons/8/84/Matplotlib_icon.svg" title="Matplotlib" alt="Matplotlib" width="40" height="40"/>&nbsp;
</div>

<p></p>

### Info:
Retail sales analysis to identify trends and patterns.

### Data:
This dataset contains information about customer transactions and product details.  

### Goal:
Provide retail analysis to make better decisions about product, pricing, promotions, inventory, customer needs other aspects of business.

### Steps to achieve the goal:
- Pre-analysis
- Exploratory Data Analysis
- ABC-analysis
- XYZ-analysis
- ABC/XYZ-analysis
- Sales analysis

### Results:
#### 1. ABC-analysis
The analysis showed that:
- 27% of users brought 80% of revenue
- 21% of products generated 80% of revenue.
These results are in good agreement with the Pareto principle.

The results of this analysis can be used to select a further strategy. For example, in the case of goods:
- Products from group A can be further promoted - mentioned in mailing lists, push notifications, and promotions with them.
- Products from group B should be purchased in quantities to cover demand.
- Products from group C should either be stopped selling or their stocks reduced.

#### 2. XYZ-analysis  
The analysis showed that:
- for both users and products, purchases with a large coefficient of variability or single purchases predominate, that is, it is difficult to make any forecasts for them
- you need to pay attention to objects with stable demand

#### 3. ABC/XYZ-analysis
**Users:**  
- The largest profits were made by users from the AZ group, these users are unstable, but they have potential, we need to work hard with these users, bringing them to constant large volumes, the share of these users is large.
- The majority of users are in the CZ group, these users are unpredictable, perhaps they are new users, or they had seasonal purchases, you need to study them more carefully.
- There are a small number of AX’s best users; you need to work with them more carefully, remind them of yourself, make discounts and offers.
- The income from the CX group is small, these users buy constantly, but not much.
- There is also a large income from users of the BZ group, their share is decent, these users are unstable, but they generate income, we need to increase the volume of purchases from them.

Additionally, a cohort analysis of users and RFM analysis should be performed.

**Goods:**  
- The groups AZ and BZ bring us the most income - with high revenue they are characterized by low predictability. An attempt to ensure guaranteed availability for all products of these groups only through excess safety inventory will lead to the fact that the company's average inventory will increase significantly. Therefore, the ordering system for products in this group should be revised:
  - transfer part of the goods to an ordering system with a constant order amount
  - ensure more frequent deliveries of some goods
  - increase the frequency of monitoring
  - entrust work with this group of products to the most experienced manager of the company
- Most of the goods belong to the CZ group - all new goods, goods of spontaneous demand, supplied to order fall into it. Some of these products can be removed from the assortment, while others need to be regularly monitored. These products require additional analysis, since there is revenue from them.
- AY can go to group AX - the most valuable goods; in our case, there are no goods in this group. Demand for them can be stimulated: for example, by placing these products on the first pages of catalogs. But the quantity of these goods is very small.
- The remaining groups bring minimal revenue, it is worth conducting additional research.

There is very low predictability, it is necessary to stimulate demand for goods.
