import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import tabulate as tb

from google.cloud import bigquery
client = bigquery.Client()
tables = [
    "distribution_centers",
    "orders",
    "products",
    "users",
    "inventory_items",
    "order_items",
    "events"
]
# Loading all tables into the dictionary
dataframes = {}

for table in tables:
    query = f"SELECT * FROM `bigquery-public-data.thelook_ecommerce.{table}`"
    dataframes[table] = client.query(query).to_dataframe()
    print(f"✅ Table loaded: {table}")

# Unpacking a dictionary into a variable
distribution_centers, orders, products, users, inventory_items, order_items, events = dataframes.values()

# print(orders.head().to_string())
# print(orders.info())

              # Create a for loop to change all the "_at" columns into datetime columns
dateTime_columns = ['created_at', 'returned_at', 'delivered_at', 'shipped_at']
for column in dateTime_columns:
    order_items[column] = pd.to_datetime(order_items[column],format = 'mixed')
    order_items = order_items.dropna(subset=dateTime_columns, how='all')
# print(order_items.info())

# print(order_items[['created_at', 'shipped_at', 'delivered_at', 'returned_at']].head().to_string())

                        #Join the Orders and Order_items data
      # Leave only columns from 'orders', which are not in 'order_items'
orders_columns_to_add = [col for col in orders.columns if col not in order_items.columns]
      # We merge, adding only the necessary columns from 'orders'
orders_orderitems = pd.merge(order_items, orders[["order_id", "user_id"] + orders_columns_to_add],
                             on=['order_id', 'user_id'],
                             how='left')
print(orders_orderitems.info())

                      # Join the Orders_products and Products  data

products_columns_to_add = [col for col in products.columns if col != 'id']
orders_products = pd.merge(orders_orderitems, products[['id'] + products_columns_to_add],
                           left_on='product_id',
                           right_on='id',
                           how='left')

orders_products = orders_products.drop(columns=['id_y'])
print(orders_products.info())
print(orders_products.head().to_string())

                      # Join the all_order  data

all_order_data = pd.merge(orders_products, users,
                            left_on='user_id',
                            right_on='id',
                            how='left',
                             suffixes=('_orders', '_user'))
print(all_order_data.head().to_string())
print(all_order_data.info())

cancelled_orders = all_order_data[all_order_data['status'] == 'Cancelled'].copy()
returned_orders = all_order_data[all_order_data['status'] == 'Returned'].copy()
complete_orders = all_order_data[all_order_data['status'].isin(['Complete', 'Shipped'])].copy()

print(f"Number of canceled orders: {len(cancelled_orders)}")
print(f"Number of returned orders: {len(returned_orders)}")
print(f"Number of completed or shipped orders: {len(complete_orders)}")

top_selling_products = (
    complete_orders.groupby(['product_id','name'])
    .size()
    .reset_index(name="total_sold")
    .sort_values('total_sold', ascending=False)
    )
print(top_selling_products.head().to_string())

                                        #  Summarizing the Order data

complete_orders['total_profit'] = complete_orders['sale_price'] - complete_orders['cost']
complete_orders_summary = complete_orders.groupby(['product_id', 'name', 'category', 'department']).agg(
    total_quantity=('product_id', 'size'),
    product_cost=('cost', 'sum'),
    total_revenue=('sale_price', 'sum'),
    total_profit=('total_profit', 'sum')
).reset_index().sort_values('total_revenue', ascending=False)

    #Аdding profit_per_unit to identify which units are the most profitable
complete_orders_summary['profit_per_unit'] = complete_orders_summary['total_profit'] / complete_orders_summary['total_quantity']
complete_orders_summary['profit_per_unit'] = complete_orders_summary['profit_per_unit'].fillna(0)
print(tb.tabulate(complete_orders_summary.head(10), headers='keys', tablefmt='fancy_grid'))
print(complete_orders_summary.info())

    # Get the highest value of a few categories.
def print_highest_in_category(df, column_name, metric_name):
    # Find the index of the highest value for the given column
      max = df[column_name].idxmax()
      highest_row = df.loc[max]
      print(f"Category with Highest {metric_name}:")
      print(f"Product Category: {highest_row['category']}, Product Department: {highest_row['department']}, {metric_name}: {highest_row[column_name]}\n")

# Execute the function for each metric
print_highest_in_category(complete_orders_summary, 'profit_per_unit', 'Profit per Unit')
print_highest_in_category(complete_orders_summary, 'total_revenue', 'Total Revenue')
print_highest_in_category(complete_orders_summary, 'total_profit', 'Total Profit')
print_highest_in_category(complete_orders_summary, 'total_quantity', 'Total Quantity')

                                 # Profits and Revenue
                       #Profit and Revenue breakdown by department
revenue_profit_summary = complete_orders_summary.groupby('department').agg(
    total_revenue_sum=('total_revenue', 'sum'),
    total_profit_sum=('total_profit', 'sum')
).reset_index()
pd.options.display.float_format = '{:,.2f}'.format  # Відображати числа з роздільниками і 2 знаками після коми
print(revenue_profit_summary.head(10))

                    #Breakdown by Men's department
    #Summarize the mens data to break it down by product_category.
men_summary = complete_orders[complete_orders['department'] == 'Men'].groupby(
    ['department', 'category']).agg(
    total_inventory_cost=('cost', 'sum'),
    total_revenue=('sale_price', 'sum'),
    total_profit=('total_profit', 'sum'),
    total_quantity=(('product_id', 'size'))
).reset_index().sort_values(by=['total_profit'], ascending=[False])
    # Calculate profit per unit
men_summary['profit_per_unit'] = men_summary['total_profit'] / men_summary['total_quantity']
pd.options.display.float_format = '{:,.2f}'.format
# men_summary['profit_per_unit'] = men_summary['profit_per_unit'].fillna(0) # Ensure no division by zero errors
print(men_summary.head().to_string())

            #Breakdown by Women's department
  #Summarize the women data to break it down by product_category.
women_summary = complete_orders[complete_orders['department'] == 'Women'].groupby(
    ['department', 'category']).agg(
    total_inventory_cost=('cost', 'sum'),
    total_revenue=('sale_price', 'sum'),
    total_profit=('total_profit', 'sum'),
    total_quantity=('product_id', 'size')
).reset_index().sort_values(by=['total_profit'], ascending=[False])
  # Calculate profit per unit
women_summary['profit_per_unit'] = women_summary['total_profit'] / women_summary['total_quantity']
pd.options.display.float_format = '{:,.2f}'.format
#women_summary['profit_per_unit'] = women_summary['profit_per_unit'].fillna(0) # Ensure no division by zero errors
print(women_summary.head(15).to_string())

complete_orders['year_month'] = complete_orders['created_at_orders'].dt.tz_localize(None).dt.to_period('M')

    # Aggregate profit by year and month

profit_trends = complete_orders.groupby(['year_month', 'department']).agg(
    total_profit = ('total_profit', 'sum'),
    total_revenue = ('retail_price', 'sum')
).reset_index()

print(profit_trends.head().to_string())

                                    # Profit trend by department
plot_data = complete_orders[['year_month', 'department', 'total_profit']]
plot_data_grouped = plot_data.groupby(['year_month', 'department'])['total_profit'].sum().unstack().fillna(0)
# Plotting
plot_data_grouped.plot(kind='bar', stacked=True, figsize=(14, 8))
plt.title('Profit Trends Over Time by Product Department')
plt.xlabel('Year and Month')
plt.ylabel('Total Profit')
plt.xticks(rotation=45)
plt.legend(title='Department')
plt.tight_layout()
plt.show()

    # Preparing data for plotting both profit and revenue trends over time for Men and Women departments
plot_data_combined = profit_trends.pivot(index='year_month', columns='department', values=['total_profit', 'total_revenue'])
fig, ax1 = plt.subplots(figsize=(14, 8))
plot_data_combined['total_profit'].plot(ax=ax1, linewidth=2, linestyle='dashed')
ax1.set_ylabel('Total Profit', color='black')
ax1.tick_params(axis='y', labelcolor='black')
ax2 = ax1.twinx()
plot_data_combined['total_revenue'].plot(ax=ax2, linewidth=2)
ax2.set_ylabel('Total Revenue', color='black')
ax2.tick_params(axis='y', labelcolor='black')
ax1.legend(['Men - Profit', 'Women - Profit'], loc='upper left')
ax2.legend(['Men - Revenue', 'Women - Revenue'], loc='upper right')
fig.tight_layout()
plt.show()

                            # Total Revenue by Country per Year
complete_orders['year'] = complete_orders['created_at_orders'].dt.tz_localize(None).dt.to_period('Y')
revenue_by_year_country = complete_orders.groupby(['year', 'country'])['retail_price'].sum().unstack(fill_value=0)

custom_colors = ['#4B0082', '#33FF57', '#3357FF', '#003153','#01796F','#D1E231','#FADFAD','#C7FCEC','#FADADD','#ABCDEF','#D1EDF2','#997A8D','#CCFF00','#C8A2C8','#C3B091','#2A3439']
ax = revenue_by_year_country.plot(kind='bar', stacked=True, color=custom_colors, figsize=(14, 8))
ax.yaxis.set_major_formatter(ticker.StrMethodFormatter('${x:,.0f}'))  # Formatting y-axis as full dollar amounts

plt.title('Total Revenue by Country per Year')
plt.xlabel('Year')
plt.ylabel('Total Revenue')
plt.legend(title='Country', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

                            #Country_analysis
country_analysis = complete_orders.groupby('country').agg(
    transactions=('order_id', 'nunique'),  # Count unique orders as transactions
   total_revenue=('retail_price', 'sum')
).reset_index().sort_values(by='transactions', ascending=False)

countries_by_revenue = country_analysis.sort_values(by='total_revenue', ascending=False)
pd.options.display.float_format = '{:,.2f}'.format
print(countries_by_revenue)

                # Adjusting the data to aggregate by country and by quarter

pd.set_option('display.max_columns', None)

complete_orders['created_at'] = pd.to_datetime(complete_orders['created_at_orders']).dt.tz_localize(None)
complete_orders['year_quarter'] = complete_orders['created_at_orders'].dt.tz_localize(None).dt.to_period('Q')

# Aggregate data by country and year_quarter, summing the total revenue
revenue_by_quarter = complete_orders.groupby(['country', 'year_quarter'])['retail_price'].sum().unstack(fill_value=0)

print(revenue_by_quarter.to_string ())

                              # Returned Orders
status_counts = all_order_data['status'].value_counts()

status_percentages = (status_counts / all_order_data.shape[0]) * 100

# Combining counts and percentages into a single dataframe
status_summary = pd.DataFrame({
    'Count': status_counts,
    'Percentage': status_percentages
}).reset_index().rename(columns={'index': 'Status'})
print(status_summary)

                    #Most returned by Product

most_returned = returned_orders['name'].value_counts().reset_index()
most_returned.columns = ['Product Name', 'Return Count']
print(most_returned.head(10))

                       #Most returned by Brand
most_returned_brand = returned_orders.groupby('brand')['product_id'].size().reset_index()
most_returned_brand.columns = ['Product Brand', 'Return Count']
most_returned_brand = most_returned_brand.sort_values(by='Return Count', ascending=False).head(5)
print(most_returned_brand)

                        #Most returned by department
most_returned_department = returned_orders['department'].value_counts().reset_index()
most_returned_department.columns = ['Product Department', 'Return Count']
print(most_returned_department.head())

                      #Most returned by product_category
most_returned_pc = returned_orders.groupby('category')['product_id'].size().reset_index()
most_returned_pc.columns = ['Category', 'Return Count']
most_returned_pc = most_returned_pc.sort_values(by='Return Count', ascending=False)
print(most_returned_pc)

                        #Most returned by distribution center
most_returned_dc = returned_orders.groupby('distribution_center_id')['product_id'].size().reset_index()
most_returned_dc.columns = ['Distribution Center', 'Return Count']
most_returned_dc = most_returned_dc.sort_values(by='Return Count', ascending=False).head(10)
print(most_returned_dc)

                            # Filter the dataset for 'Allegra K' brand
allegra_k_returns = returned_orders[returned_orders['brand'] == 'Allegra K']
allegra_k_completed = complete_orders[complete_orders['brand'] == 'Allegra K']

total_inventory_cost_allegra_k = allegra_k_returns['cost'].sum() * allegra_k_returns['product_id'].size
total_lost_revenue_allegra_k = allegra_k_returns['sale_price'].sum() * allegra_k_returns['product_id'].size
total_sold_allega_k = allegra_k_completed['product_id'].size
total_returned_allegra_k = allegra_k_returns['product_id'].size

# Find which 'Allegra K' products are being returned the most
most_returned_allegra_k_products = allegra_k_returns['name'].value_counts().head(10)

print ("Total Inventory Cost:", total_inventory_cost_allegra_k)
print ("Total Lost Revenue:", total_lost_revenue_allegra_k)
print ("Total Returned:", total_returned_allegra_k)
print ("Total Sold:", total_sold_allega_k)
print(most_returned_allegra_k_products)

                                         # Cancelled Orders
# print(cancelled_orders.head())
print(cancelled_orders['name'].value_counts().head())
print(cancelled_orders['country'].value_counts().head())
print(cancelled_orders['department'].value_counts())
print(cancelled_orders['category'].value_counts().head())
print(cancelled_orders['traffic_source'].value_counts())

                                # Events data review
     #Sort the values by session_id and then the sequence_number. This is the order of activity on the site.
events = events.sort_values(by=['session_id', 'sequence_number'])
print(events.head(10).to_string())

event_types = events['event_type'].unique()
print(event_types)

#Running the below to convert 'created_at' to datetime and convert errors to NaT(Not a Time)
events['created_at'] = pd.to_datetime(events['created_at'], format = 'mixed')
missing_dates_count = events['created_at'].isna().sum()
print(f"Number of rows without date: {missing_dates_count}")

#If there were rows with incorrect dates then
# events = events.dropna(subset=['created_at'])
# print(events.info)
# # print(events['created_at'].isna().sum())
# # Ensure 'created_at' is converted to datetime format
# events['created_at'] = pd.to_datetime(events['created_at'])
# # Check the data types again to confirm the conversion
# updated_dtypes = events.dtypes
# # print(updated_dtypes['created_at'])
# missing_values = events.isnull().sum()
# print(missing_values)

                    #Lets see if we can figure out the way that these users best found their way to the website.
countby_traffic_eventType = events.groupby(['traffic_source', 'event_type']).size().reset_index(name='count')
countby_traffic_eventType = countby_traffic_eventType.sort_values('count', ascending=False)

count_by_traffic_source = events.groupby(['traffic_source']).size().reset_index(name='count')
count_by_traffic_source = count_by_traffic_source.sort_values('count', ascending=False)
print(countby_traffic_eventType.head(10))
print(count_by_traffic_source.head())

             # Ensure purchase_events only contains 'purchase' event_type
purchase_events = countby_traffic_eventType[countby_traffic_eventType['event_type'] == 'purchase']

               # Merge purchase counts with total counts on traffic_source
conversion_df = pd.merge(count_by_traffic_source, purchase_events[['traffic_source', 'count']],
                      on='traffic_source',
                       how='left',
                       suffixes=('_total', '_purchase'))

# conversion_df['count_purchase'].fillna(0, inplace=True)           # Fill NaN values with 0 for traffic sources without any purchases

# Calculate conversion percentage
conversion_df['conversion_percentage'] = (conversion_df['count_purchase'] / conversion_df['count_total']) * 100

# Sort by conversion_percentage for better visibility, if needed
conversion_df = conversion_df.sort_values('count_purchase', ascending=False)

print(conversion_df)

#Lets see where most of the events stemmed from. Omitting the state as the chinese cities don't have a state listed.
city_counts = events.groupby(['city']).size().reset_index(name='count')
city_counts = city_counts.sort_values('count', ascending=False)

print(city_counts.head(10))

                       # Tracking active sessions

# Yearly Trend Analysis: Aggregate event counts by year and month
yearly_monthly_events = events.groupby([events['created_at'].dt.year, events['created_at'].dt.month_name()]).size().unstack().fillna(0)

month_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
yearly_monthly_events = yearly_monthly_events[month_order]

# Plotting the yearly trend for each month
plt.figure(figsize=(14, 8))
sns.lineplot(data=yearly_monthly_events, dashes=False, markers=True)
plt.title('Yearly Trend with One Line for Each Month')
plt.xlabel('Year')
plt.ylabel('Event Count')
plt.xticks(rotation=45)
plt.legend(title='Month', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
                     # Lets now check out the number visitors the site gets each month.
events['created_at'] = pd.to_datetime(events['created_at'])
events['year_month'] = events['created_at'].dt.tz_localize(None).dt.to_period('M')
events['date'] = events['created_at'].dt.date
daily_sessions = events.groupby(['year_month', 'date'])['session_id'].nunique().reset_index(name='counts')
monthly_sessions = daily_sessions.groupby('year_month')['counts'].mean().reset_index(name='avg_sessions_per_month')

print(monthly_sessions.head())

                   # Average Sessions Per Month
# Convert 'year_month' back to datetime for plotting
monthly_sessions['year_month'] = monthly_sessions['year_month'].dt.to_timestamp()

plt.figure(figsize=(12, 6))
plt.plot(monthly_sessions['year_month'], monthly_sessions['avg_sessions_per_month'], marker='o', linestyle='-', color='blue')
plt.title('Average Sessions Per Month')
plt.xlabel('Month')
plt.ylabel('Average Sessions')
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()  # Adjust layout to make room for the rotated x-axis labels
plt.show()
