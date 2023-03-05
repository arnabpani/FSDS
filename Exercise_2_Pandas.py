# -*- coding: utf-8 -*-
"""
Created on Mon Feb 13 17:37:05 2023

@author: arnab
"""

#Read Dataframe
import pandas as pd

final_df = pd.DataFrame()

df_Agent_Login = pd.read_csv(r"C:\Users\arnab\Documents\DATA SCIENCE\DATASETS\AgentLogingReport.csv")
df_Agent_Perform = pd.read_csv(r"C:\Users\arnab\Documents\DATA SCIENCE\DATASETS\AgentPerformance.csv")

df_Agent_Perform.rename(columns = {'Agent Name':'Agent'}, inplace = True)


df_Agent_Login["Duration"] = pd.to_datetime(df_Agent_Login.Duration, infer_datetime_format=True, errors='coerce')

# final_df['Total Working Hours'] = df_Agent_Login.groupby('Agent')['Duration'].sum()

df_Agent_Login['Date'] = pd.to_datetime(df_Agent_Login['Date'], dayfirst = True).dt.date
df_Agent_Perform['Date'] = pd.to_datetime(df_Agent_Perform['Date'], dayfirst = True).dt.date

# df = df_Agent_Login.merge(df_Agent_Perform, how='outer', on=['Agent','Date']).drop(['SL No_x', 'SL No_y'], axis=1)
# df[["Total Feedback", "Total Chats", "Average Rating"]] = df[["Total Feedback", "Total Chats", "Average Rating"]].fillna(0)
# df[["Average Response Time", "Average Resolution Time"]] = df[["Average Response Time", "Average Resolution Time"]].fillna('0:00:00')

# 1 .Find out there avarage rating on weekly basis keep this in a mind that they take two days of leave


# 2 .Total working days for each agents
final_df['Total Working Days'] = df_Agent_Login.drop_duplicates(subset = ['Agent','Date'], keep = 'first').groupby('Agent')['Date'].count()

# 3. Total query that each agent has taken 
final_df = final_df.merge(df_Agent_Perform.groupby('Agent').sum()['Total Chats'], how='outer', on=['Agent']).fillna(0)

# 4. total Feedback that you have received 
final_df = final_df.merge(df_Agent_Perform.groupby('Agent').sum()['Total Feedback'], how='outer', on=['Agent']).fillna(0)

# 5. Agent name who have average rating between 3.5 to 4
df_Expected_Rating = df_Agent_Perform[(df_Agent_Perform['Average Rating'] > 3.5) & (df_Agent_Perform['Average Rating'] < 4)]['Agent']

# 6. Agent name who have rating less then 3.5
print(df_Agent_Perform[(df_Agent_Perform['Average Rating'] < 3.5)]['Agent'])

# 7. agent name who have rating more then 4.5
print(df_Agent_Perform[(df_Agent_Perform['Average Rating'] > 4.5)]['Agent'])

# 8. how many feedaback agents have received more then 4.5 average
print(df_Agent_Perform[(df_Agent_Perform['Average Rating'] > 4.5)]['Agent'].count())

# 9. average weekly response time for each agent

# 10. average weekely resolution time for each agents

# 11. list of all agents name

# 12. percentage of chat on which they have received a feedback

# 13. Total contributation hour for each and every agents weekly basis

# 14. total percentage of active hour for a month
