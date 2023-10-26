# -*- coding: utf-8 -*-
"""
Created on Wed Oct  4 23:53:31 2023

@author: shua
"""

import pandas as pd
import numpy as np

df = pd.read_csv("D:/Sang/ISONYLossesDeducible.csv",dtype=str) ##dtype=dtype)

def convert_int(x):
    try:
        return int(x)
    except:
        return 

numeric_column=['calc_claim','calc_paid_claim','Crime2','Deductible','CovA']

for x in numeric_column:
    df[x]=df[x].apply(convert_int)
    
 #filter out water peril only   
df_water=df[df['Crime2']>0]
print('df.shape',df.shape)
print('df_water.shape',df_water.shape)

df_water['net_loss']=df_water['Crime2']

#Ceate a list of cut points 
cut_points = [-5000, 0, 225000, 250000, 275000, 300000, 325000, 350000, 375000, 400000, 425000, 450000, 475000, 500000, 550000, 600000, 650000, 700000, 750000, 800000, 900000, 1000000, float('inf')] 

# Create a list of labels 
labels = ['Unknown', '0-225K', '225-2.50K', '250-275K', '275-300K', '300-325K', '325-350K', '350-375K', '375-400K', '400-425K', '425-450K', '450-475K', '475-500K', '500-550K', '550-600K', '600-650K', '650-700K', '700-750K', '750-800K', '800-900K', '900-1000K', '1000K+'] 

df_water['CovA_num']=pd.to_numeric(df_water['CovA'],errors='coerce')

df_water.info()

df_water.loc[:,'CovA_cat']=pd.cut(df_water['CovA_num'],bins=cut_points,labels=labels,right=True)

df_water.info()

display(df_water[['CovA','CovA_num','CovA_cat','net_loss']])

# Step 1: Calculate net_loss_100_at_200 
def step1(row): 
    deductible = row['Deductible']
    net_loss = row['net_loss'] 
    num_of_claims = row['calc_claim'] 
    if deductible == 100: 
        return max(net_loss - num_of_claims * 100, 0) 
    else: return 0 
    
df_water['net_loss_100_to_200'] = df_water.apply(step1, axis=1) 

display(df_water[df_water['Deductible']==100][['net_loss_100_to_200','net_loss','Deductible','calc_claim']])

df_100_to_200 =df_water[df_water['Deductible']==100].groupby(['CovA_cat']).agg({'net_loss_100_to_200':'sum','net_loss':'sum'}).reset_index()

display(df_100_to_200)
df_100_to_200['ler']= 1- df_100_to_200['net_loss_100_to_200']/df_100_to_200['net_loss']

df_100_to_200_T = df_100_to_200.T

df_100_to_200_T = df_100_to_200_T.fillna(0)
display(df_100_to_200_T)

# Step 2: Calculate net_loss_200_at_250
def step2(row): 
    deductible = row['Deductible']
    net_loss = row['net_loss'] 
    num_of_claims = row['calc_claim'] 
    if deductible == 200: 
        netloss = max(net_loss - num_of_claims * 50, 0) 
    elif deductible == 100:
        netloss = max(net_loss - num_of_claims * 150,0)
    else: 
        netloss = 0
    return netloss
        
    
df_water['net_loss_200_to_250'] = df_water.apply(step2, axis=1) 
list_of_values=[100,200]

display(df_water[df_water['Deductible'].isin(list_of_values)][['net_loss_200_to_250','net_loss','Deductible','calc_claim']])

df_200_to_250 =df_water[df_water['Deductible'].isin(list_of_values)].groupby(['CovA_cat']).agg({'net_loss_200_to_250':'sum','net_loss':'sum'}).reset_index()

display(df_200_to_250)
df_200_to_250['ler']= 1- df_200_to_250['net_loss_200_to_250']/df_200_to_250['net_loss']

df_200_to_250_T = df_200_to_250.T

df_200_to_250_T = df_200_to_250_T.fillna(0)

display(df_200_to_250_T)

df_200_to_250_T=df_200_to_250_T.drop(index='CovA_cat')

combine=pd.concat([df_100_to_200_T,df_200_to_250_T],axis=0)

combine.to_pickle('D:\Sang\combine_elr.pickle')

display(combine)

combine.to_csv('D:\Sang\combine.csv')

# Step 3: Calculate net_loss_250_at_500 

def step3(row): 
    deductible = row['Deductible'] 
    net_loss = row['net_loss'] 
    num_of_claims = row['calc_claim'] 
    if deductible == 250: 
        netloss = max(net_loss - num_of_claims * 250, 0)
    elif deductible == 200: 
        netloss = max(net_loss - num_of_claims * 300, 0)   
    elif deductible == 100:
        netloss = max(net_loss - num_of_claims * 400,0)
    else: 
        netloss = 0
              
    return netloss
    

df_water['net_loss_250_to_500'] = df_water.apply(lambda row: step3(row), axis=1)   

df_water.columns    

list_of_values=[100,200,250]
display(df_water[df_water['Deductible'].isin(list_of_values)][['net_loss_250_to_500','Deductible','net_loss','calc_claim']])

df_250_to_500 =df_water[df_water['Deductible'].isin(list_of_values)].groupby(['CovA_cat']).agg({'net_loss_250_to_500':'sum','net_loss':'sum'}).reset_index()

display(df_250_to_500)
df_250_to_500['ler']= 1- df_250_to_500['net_loss_250_to_500']/df_250_to_500['net_loss']

df_250_to_500_T = df_250_to_500.T

df_250_to_500_T = df_250_to_500_T.fillna(0)
display(df_250_to_500_T)


df_250_to_500_T=df_250_to_500_T.drop(index='CovA_cat')

combine=pd.read_pickle('D:\Sang\combine_elr.pickle')
combine=pd.concat([combine,df_250_to_500_T],axis=0)

combine.to_pickle('D:\Sang\combine_elr.pickle')

display(combine)

combine.to_csv('D:\Sang\combine_elr.csv')

# Step 4: Calculate net_loss_500_to_750 

def step4(row): 
    deductible = row['Deductible'] 
    net_loss = row['net_loss'] 
    num_of_claims = row['calc_claim'] 
    if deductible == 500:
        netloss = max(net_loss - num_of_claims * 250, 0)
    elif deductible == 250: 
        netloss = max(net_loss - num_of_claims * 500, 0)
    elif deductible == 200:
        netloss = max(net_loss - num_of_claims * 550, 0)
    elif deductible == 100:
        netloss = max(net_loss - num_of_claims * 650,0)
    else: 
        netloss = 0
              
    return netloss
    

df_water['net_loss_500_to_750'] = df_water.apply(lambda row: step4(row), axis=1)   

df_water.columns    

list_of_values=[100,200,250,500]
display(df_water[df_water['Deductible'].isin(list_of_values)][['net_loss_500_to_750','Deductible','net_loss','calc_claim']])

df_500_to_750 =df_water[df_water['Deductible'].isin(list_of_values)].groupby(['CovA_cat']).agg({'net_loss_500_to_750':'sum','net_loss':'sum'}).reset_index()

display(df_500_to_750)
df_500_to_750['ler']= 1- df_500_to_750['net_loss_500_to_750']/df_500_to_750['net_loss']

df_500_to_750_T = df_500_to_750.T

df_500_to_750_T = df_500_to_750_T.fillna(0)

display(df_500_to_750_T)

df_500_to_750_T=df_500_to_750_T.drop(index='CovA_cat')

combine=pd.read_pickle('D:\Sang\combine_elr.pickle')

combine=pd.concat([combine,df_500_to_750_T],axis=0)

combine.to_pickle('D:\Sang\combine_elr.pickle')

combine.to_csv('D:\Sang\Combine_elr.csv')

# Step 5: Calculate net_loss_750_to_1000 

def step5(row): 
    deductible = row['Deductible'] 
    net_loss = row['net_loss'] 
    num_of_claims = row['calc_claim'] 
    if deductible == 750:
        netloss = max(net_loss - num_of_claims * 250, 0)
    elif deductible == 500:
        netloss = max(net_loss - num_of_claims * 500, 0)
    elif deductible == 250: 
        netloss = max(net_loss - num_of_claims * 750, 0)
    elif deductible == 200:
        netloss = max(net_loss - num_of_claims * 800, 0)
    elif deductible == 100:
        netloss = max(net_loss - num_of_claims * 900,0)
    else: 
        netloss = 0
              
    return netloss
    

df_water['net_loss_750_to_1000'] = df_water.apply(lambda row: step5(row), axis=1)   

df_water.columns    

list_of_values=[100,200,250,500,750]
display(df_water[df_water['Deductible'].isin(list_of_values)][['net_loss_750_to_1000','Deductible','net_loss','calc_claim']])

df_750_to_1000 =df_water[df_water['Deductible'].isin(list_of_values)].groupby(['CovA_cat']).agg({'net_loss_750_to_1000':'sum','net_loss':'sum'}).reset_index()

display(df_750_to_1000)
df_750_to_1000['ler']= 1- df_750_to_1000['net_loss_750_to_1000']/df_750_to_1000['net_loss']

df_750_to_1000_T = df_750_to_1000.T

df_750_to_1000_T = df_750_to_1000_T.fillna(0)
display(df_750_to_1000_T)

df_750_to_1000_T=df_750_to_1000_T.drop(index='CovA_cat')

combine=pd.read_pickle('combine_elr.pickle')

combine=pd.concat([combine,df_750_to_1000_T],axis=0)

combine.to_pickle('combine_elr.pickle')

combine.to_csv('Combine_elr.csv')

# Step 6: Calculate net_loss_1000_to_1500 

def step6(row): 
    deductible = row['Deductible'] 
    net_loss = row['net_loss'] 
    num_of_claims = row['calc_claim'] 
    if deductible == 1000:
        netloss = max(net_loss - num_of_claims * 500, 0)   
    elif deductible == 750:
        netloss = max(net_loss - num_of_claims * 750, 0)
    elif deductible == 500:
        netloss = max(net_loss - num_of_claims * 1000, 0)
    elif deductible == 250: 
        netloss = max(net_loss - num_of_claims * 1250, 0)
    elif deductible == 200:
        netloss = max(net_loss - num_of_claims * 1300,0)
    elif deductible == 100:
        netloss = max(net_loss - num_of_claims * 1400,0)
    else: 
        netloss = 0
              
    return netloss
    

df_water['net_loss_1000_to_1500'] = df_water.apply(lambda row: step6(row), axis=1)   

df_water.columns    

list_of_values=[100,200, 250,500,750,1000]
display(df_water[df_water['Deductible'].isin(list_of_values)][['net_loss_1000_to_1500','Deductible','net_loss','calc_claim']])

df_1000_to_1500 =df_water[df_water['Deductible'].isin(list_of_values)].groupby(['CovA_cat']).agg({'net_loss_1000_to_1500':'sum','net_loss':'sum'}).reset_index()

display(df_1000_to_1500)
df_1000_to_1500['ler']= 1- df_1000_to_1500['net_loss_1000_to_1500']/df_1000_to_1500['net_loss']

df_1000_to_1500_T = df_1000_to_1500.T

df_1000_to_1500_T = df_1000_to_1500_T.fillna(0)

display(df_1000_to_1500_T)

df_1000_to_1500_T=df_1000_to_1500_T.drop(index='CovA_cat')

combine=pd.read_pickle('D:\Sang\combine_elr.pickle')

combine=pd.concat([combine,df_1000_to_1500_T],axis=0)

combine.to_pickle('D:\Sang\combine_elr.pickle')

combine.to_csv('D:\Sang\combine_elr.csv')


# Step 7: Calculate net_loss_1500_to_2000 
def step7(row): 
    deductible = row['Deductible'] 
    net_loss = row['net_loss'] 
    num_of_claims = row['calc_claim'] 
    if deductible == 1500:
        netloss = max(net_loss - num_of_claims * 500, 0)     
    elif deductible == 1000:
        netloss = max(net_loss - num_of_claims * 1000, 0)    
    elif deductible == 750:
        netloss = max(net_loss - num_of_claims * 1250, 0)
    elif deductible == 500:
        netloss = max(net_loss - num_of_claims * 1500, 0)
    elif deductible == 250: 
        netloss = max(net_loss - num_of_claims * 1750, 0)
    elif deductible == 200:
        netloss = max(net_loss - num_of_claims * 1800, 0)
    elif deductible == 100:
        netloss = max(net_loss - num_of_claims * 1900, 0)
    else: 
        netloss = 0
              
    return netloss
    

df_water['net_loss_1500_to_2000'] = df_water.apply(lambda row: step7(row), axis=1)   

df_water.columns    

list_of_values=[100,200,250,500,750,1000,1500]
display(df_water[df_water['Deductible'].isin(list_of_values)][['net_loss_1500_to_2000','Deductible','net_loss','calc_claim']])

df_1500_to_2000 =df_water[df_water['Deductible'].isin(list_of_values)].groupby(['CovA_cat']).agg({'net_loss_1500_to_2000':'sum','net_loss':'sum'}).reset_index()

display(df_1500_to_2000)
df_1500_to_2000['ler']= 1- df_1500_to_2000['net_loss_1500_to_2000']/df_1500_to_2000['net_loss']

df_1500_to_2000_T = df_1500_to_2000.T

df_1500_to_2000_T = df_1500_to_2000_T.fillna(0)

display(df_1500_to_2000_T)

df_1500_to_2000_T=df_1500_to_2000_T.drop(index='CovA_cat')

combine=pd.read_pickle('D:\Sang\combine_elr.pickle')

combine=pd.concat([combine,df_1500_to_2000_T],axis=0)

combine.to_pickle('D:\Sang\combine_elr.pickle')

combine.to_csv('D:\Sang\combine_elr.csv')

# Step 8: Calculate net_loss_2000_to_2500 
def step8(row): 
    deductible = row['Deductible'] 
    net_loss = row['net_loss'] 
    num_of_claims = row['calc_claim'] 
    if deductible == 2000:
        netloss = max(net_loss - num_of_claims * 500, 0)     
    elif deductible == 1500:
        netloss = max(net_loss - num_of_claims * 1000, 0)     
    elif deductible == 1000:
        netloss = max(net_loss - num_of_claims * 1500, 0)    
    elif deductible == 750:
        netloss = max(net_loss - num_of_claims * 1750, 0)
    elif deductible == 500:
        netloss = max(net_loss - num_of_claims * 2000, 0)
    elif deductible == 250: 
        netloss = max(net_loss - num_of_claims * 2250, 0)
    elif deductible == 200:
        netloss = max(net_loss - num_of_claims * 2300, 0)
    elif deductible == 100:
        netloss = max(net_loss - num_of_claims * 2400,0)
    else: 
        netloss = 0
              
    return netloss
    

df_water['net_loss_2000_to_2500'] = df_water.apply(lambda row: step8(row), axis=1)   

df_water.columns    

list_of_values=[100,200,250,500,750,1000,1500,2000]
display(df_water[df_water['Deductible'].isin(list_of_values)][['net_loss_2000_to_2500','Deductible','net_loss','calc_claim']])

df_2000_to_2500 =df_water[df_water['Deductible'].isin(list_of_values)].groupby(['CovA_cat']).agg({'net_loss_2000_to_2500':'sum','net_loss':'sum'}).reset_index()

display(df_2000_to_2500)
df_2000_to_2500['ler']= 1- df_2000_to_2500['net_loss_2000_to_2500']/df_2000_to_2500['net_loss']

df_2000_to_2500_T = df_2000_to_2500.T

df_2000_to_2500_T = df_2000_to_2500_T.fillna(0)

display(df_2000_to_2500_T)

df_2000_to_2500_T=df_2000_to_2500_T.drop(index='CovA_cat')

combine=pd.read_pickle('D:\Sang\combine_elr.pickle')

combine=pd.concat([combine,df_2000_to_2500_T],axis=0)

combine.to_pickle('D:\Sang\combine_elr.pickle')

combine.to_csv('D:\Sang\combine_elr.csv')

# Step 9: Calculate net_loss_2500_to_3000 
def step9(row): 
    deductible = row['Deductible'] 
    net_loss = row['net_loss'] 
    num_of_claims = row['calc_claim'] 
    if deductible == 2500:
        netloss = max(net_loss - num_of_claims * 500, 0)
    elif deductible == 2000:
        netloss = max(net_loss - num_of_claims * 1000, 0)     
    elif deductible == 1500:
        netloss = max(net_loss - num_of_claims * 1500, 0)     
    elif deductible == 1000:
        netloss = max(net_loss - num_of_claims * 2000, 0)    
    elif deductible == 750:
        netloss = max(net_loss - num_of_claims * 2250, 0)
    elif deductible == 500:
        netloss = max(net_loss - num_of_claims * 2500, 0)
    elif deductible == 250: 
        netloss = max(net_loss - num_of_claims * 2750, 0)
    elif deductible == 200:
        netloss = max(net_loss - num_of_claims * 2800, 0)
    elif deductible == 100:
        netloss = max(net_loss - num_of_claims * 2900,0)
    else: 
        netloss = 0
              
    return netloss
    

df_water['net_loss_2500_to_3000'] = df_water.apply(lambda row: step9(row), axis=1)   

df_water.columns    

list_of_values=[100,200,250,500,750,1000,1500,2000,2500]
display(df_water[df_water['Deductible'].isin(list_of_values)][['net_loss_2500_to_3000','Deductible','net_loss','calc_claim']])

df_2500_to_3000 =df_water[df_water['Deductible'].isin(list_of_values)].groupby(['CovA_cat']).agg({'net_loss_2500_to_3000':'sum','net_loss':'sum'}).reset_index()

display(df_2500_to_3000)
df_2500_to_3000['ler']= 1- df_2500_to_3000['net_loss_2500_to_3000']/df_2500_to_3000['net_loss']

df_2500_to_3000_T = df_2500_to_3000.T

df_2500_to_3000_T = df_2500_to_3000_T.fillna(0)

display(df_2500_to_3000_T)

df_2500_to_3000_T=df_2500_to_3000_T.drop(index='CovA_cat')

combine=pd.read_pickle('D:\Sang\combine_elr.pickle')

combine=pd.concat([combine,df_2500_to_3000_T],axis=0)

combine.to_pickle('D:\Sang\combine_elr.pickle')

combine.to_csv('D:\Sang\combine_elr.csv')


# Step 10: Calculate net_loss_3000_to_4000 
def step10(row): 
    deductible = row['Deductible'] 
    net_loss = row['net_loss'] 
    num_of_claims = row['calc_claim'] 
    if deductible == 3000:
        netloss = max(net_loss - num_of_claims * 1000,0)
    elif deductible == 2500:
        netloss = max(net_loss - num_of_claims * 1500, 0)
    elif deductible == 2000:
        netloss = max(net_loss - num_of_claims * 2000, 0)     
    elif deductible == 1500:
        netloss = max(net_loss - num_of_claims * 2500, 0)     
    elif deductible == 1000:
        netloss = max(net_loss - num_of_claims * 3000, 0)    
    elif deductible == 750:
        netloss = max(net_loss - num_of_claims * 3250, 0)
    elif deductible == 500:
        netloss = max(net_loss - num_of_claims * 3500, 0)
    elif deductible == 250: 
        netloss = max(net_loss - num_of_claims * 3750, 0)
    elif deductible == 200:
        netloss = max(net_loss - num_of_claims * 3800, 0)
    elif deductible == 100:
        netloss = max(net_loss - num_of_claims * 3900,0)
    else: 
        netloss = 0
              
    return netloss
    

df_water['net_loss_3000_to_4000'] = df_water.apply(lambda row: step10(row), axis=1)   

df_water.columns    

list_of_values=[100,200,250,500,750,1000,1500,2000,2500,3000]
display(df_water[df_water['Deductible'].isin(list_of_values)][['net_loss_3000_to_4000','Deductible','net_loss','calc_claim']])

df_3000_to_4000 =df_water[df_water['Deductible'].isin(list_of_values)].groupby(['CovA_cat']).agg({'net_loss_3000_to_4000':'sum','net_loss':'sum'}).reset_index()

display(df_3000_to_4000)
df_3000_to_4000['ler']= 1- df_3000_to_4000['net_loss_3000_to_4000']/df_3000_to_4000['net_loss']

df_3000_to_4000_T = df_3000_to_4000.T

df_3000_to_4000_T = df_3000_to_4000_T.fillna(0)

display(df_3000_to_4000_T)

df_3000_to_4000_T=df_3000_to_4000_T.drop(index='CovA_cat')

combine=pd.read_pickle('D:\Sang\combine_elr.pickle')

combine=pd.concat([combine,df_3000_to_4000_T],axis=0)

combine.to_pickle('D:\Sang\combine_elr.pickle')

combine.to_csv('D:\Sang\combine_elr.csv')


# Step 11: Calculate net_loss_4000_to_5000 
def step11(row): 
    deductible = row['Deductible'] 
    net_loss = row['net_loss'] 
    num_of_claims = row['calc_claim'] 
    if deductible == 4000:
        netloss = max(net_loss - num_of_claims * 1000, 0)
    elif deductible == 3000:
        netloss = max(net_loss - num_of_claims * 2000,0)
    elif deductible == 2500:
        netloss = max(net_loss - num_of_claims * 2500, 0)
    elif deductible == 2000:
        netloss = max(net_loss - num_of_claims * 3000, 0)     
    elif deductible == 1500:
        netloss = max(net_loss - num_of_claims * 3500, 0)     
    elif deductible == 1000:
        netloss = max(net_loss - num_of_claims * 4000, 0)    
    elif deductible == 750:
        netloss = max(net_loss - num_of_claims * 4250, 0)
    elif deductible == 500:
        netloss = max(net_loss - num_of_claims * 4500, 0)
    elif deductible == 250: 
        netloss = max(net_loss - num_of_claims * 4750, 0)
    elif deductible == 200:
        netloss = max(net_loss - num_of_claims * 4800, 0)
    elif deductible == 100:
        netloss = max(net_loss - num_of_claims * 4900,0)
    else: 
        netloss = 0
              
    return netloss
    

df_water['net_loss_4000_to_5000'] = df_water.apply(lambda row: step11(row), axis=1)   

df_water.columns    

list_of_values=[100,200,250,500,750,1000,1500,2000,2500,3000,4000]
display(df_water[df_water['Deductible'].isin(list_of_values)][['net_loss_4000_to_5000','Deductible','net_loss','calc_claim']])

df_4000_to_5000 =df_water[df_water['Deductible'].isin(list_of_values)].groupby(['CovA_cat']).agg({'net_loss_4000_to_5000':'sum','net_loss':'sum'}).reset_index()

display(df_4000_to_5000)
df_4000_to_5000['ler']= 1- df_4000_to_5000['net_loss_4000_to_5000']/df_4000_to_5000['net_loss']

df_4000_to_5000_T = df_4000_to_5000.T

df_4000_to_4000_T = df_4000_to_5000_T.fillna(0)

display(df_4000_to_5000_T)

df_4000_to_5000_T=df_4000_to_5000_T.drop(index='CovA_cat')

combine=pd.read_pickle('D:\Sang\combine_elr.pickle')

combine=pd.concat([combine,df_4000_to_5000_T],axis=0)

combine.to_pickle('D:\Sang\combine_elr.pickle')

combine.to_csv('D:\Sang\combine_elr.csv')

# Step 12: Calculate net_loss_5000_to_7500 
def step12(row): 
    deductible = row['Deductible'] 
    net_loss = row['net_loss'] 
    num_of_claims = row['calc_claim'] 
    if deductible == 5000:
        netloss = max(net_loss - num_of_claims * 2500,0)  
    elif deductible == 4000:
        netloss = max(net_loss - num_of_claims * 3500, 0)
    elif deductible == 3000:
        netloss = max(net_loss - num_of_claims * 4500,0)
    elif deductible == 2500:
        netloss = max(net_loss - num_of_claims * 5000, 0)
    elif deductible == 2000:
        netloss = max(net_loss - num_of_claims * 5500, 0)     
    elif deductible == 1500:
        netloss = max(net_loss - num_of_claims * 6000, 0)     
    elif deductible == 1000:
        netloss = max(net_loss - num_of_claims * 6500, 0)    
    elif deductible == 750:
        netloss = max(net_loss - num_of_claims * 6750, 0)
    elif deductible == 500:
        netloss = max(net_loss - num_of_claims * 7000, 0)
    elif deductible == 250: 
        netloss = max(net_loss - num_of_claims * 7250, 0)
    elif deductible == 200:
        netloss = max(net_loss - num_of_claims * 7300, 0)
    elif deductible == 100:
        netloss = max(net_loss - num_of_claims * 7400,0)
    else: 
        netloss = 0
              
    return netloss
    

df_water['net_loss_5000_to_7500'] = df_water.apply(lambda row: step12(row), axis=1)   

df_water.columns    

list_of_values=[100,200,250,500,750,1000,1500,2000,2500,3000,4000,5000]
display(df_water[df_water['Deductible'].isin(list_of_values)][['net_loss_5000_to_7500','Deductible','net_loss','calc_claim']])

df_5000_to_7500 =df_water[df_water['Deductible'].isin(list_of_values)].groupby(['CovA_cat']).agg({'net_loss_5000_to_7500':'sum','net_loss':'sum'}).reset_index()

display(df_5000_to_7500)
df_5000_to_7500['ler']= 1- df_5000_to_7500['net_loss_5000_to_7500']/df_5000_to_7500['net_loss']

df_5000_to_7500_T = df_5000_to_7500.T

df_5000_to_7500_T = df_5000_to_7500_T.fillna(0)

display(df_5000_to_7500_T)

df_5000_to_7500_T=df_5000_to_7500_T.drop(index='CovA_cat')

combine=pd.read_pickle('D:\Sang\combine_elr.pickle')

combine=pd.concat([combine,df_5000_to_7500_T],axis=0)

combine.to_pickle('D:\Sang\combine_elr.pickle')

combine.to_csv('D:\Sang\combine_elr.csv')

# Step 13: Calculate net_loss_7500_to_10000 
def step13(row): 
    deductible = row['Deductible'] 
    net_loss = row['net_loss'] 
    num_of_claims = row['calc_claim'] 
    if deductible == 7500:
        netloss = max(net_loss - num_of_claims * 2500,0)
    elif deductible == 5000:
        netloss = max(net_loss - num_of_claims * 5000,0)  
    elif deductible == 4000:
        netloss = max(net_loss - num_of_claims * 6000, 0)
    elif deductible == 3000:
        netloss = max(net_loss - num_of_claims * 7000,0)
    elif deductible == 2500:
        netloss = max(net_loss - num_of_claims * 7500, 0)
    elif deductible == 2000:
        netloss = max(net_loss - num_of_claims * 8000, 0)     
    elif deductible == 1500:
        netloss = max(net_loss - num_of_claims * 8500, 0)     
    elif deductible == 1000:
        netloss = max(net_loss - num_of_claims * 9000, 0)    
    elif deductible == 750:
        netloss = max(net_loss - num_of_claims * 9250, 0)
    elif deductible == 500:
        netloss = max(net_loss - num_of_claims * 9500, 0)
    elif deductible == 250: 
        netloss = max(net_loss - num_of_claims * 9750, 0)
    elif deductible == 200:
        netloss = max(net_loss - num_of_claims * 9800, 0)
    elif deductible == 100:
        netloss = max(net_loss - num_of_claims * 9900,0)
    else: 
        netloss = 0
              
    return netloss
    

df_water['net_loss_7500_to_10000'] = df_water.apply(lambda row: step13(row), axis=1)   

df_water.columns    

list_of_values=[100,200,250,500,750,1000,1500,2000,2500,3000,4000,5000, 7500]
display(df_water[df_water['Deductible'].isin(list_of_values)][['net_loss_7500_to_10000','Deductible','net_loss','calc_claim']])

df_7500_to_10000 =df_water[df_water['Deductible'].isin(list_of_values)].groupby(['CovA_cat']).agg({'net_loss_7500_to_10000':'sum','net_loss':'sum'}).reset_index()

display(df_7500_to_10000)
df_7500_to_10000['ler']= 1- df_7500_to_10000['net_loss_7500_to_10000']/df_7500_to_10000['net_loss']

df_7500_to_10000_T = df_7500_to_10000.T

df_7500_to_10000_T = df_7500_to_10000_T.fillna(0)

display(df_7500_to_10000_T)

df_7500_to_10000_T=df_7500_to_10000_T.drop(index='CovA_cat')

combine=pd.read_pickle('D:\Sang\combine_elr.pickle')

combine=pd.concat([combine,df_7500_to_10000_T],axis=0)

combine.to_pickle('D:\Sang\combine_elr.pickle')

combine.to_csv('D:\Sang\combine_elr.csv')


# Step 14: Calculate net_loss_10000_to_25000 
def step14(row): 
    deductible = row['Deductible'] 
    net_loss = row['net_loss'] 
    num_of_claims = row['calc_claim'] 
    if deductible == 10000:
        netloss = max(net_loss - num_of_claims * 15000,0)
    elif deductible == 7500:
        netloss = max(net_loss - num_of_claims * 17500,0)
    elif deductible == 5000:
        netloss = max(net_loss - num_of_claims * 20000,0)  
    elif deductible == 4000:
        netloss = max(net_loss - num_of_claims * 21000, 0)
    elif deductible == 3000:
        netloss = max(net_loss - num_of_claims * 22000,0)
    elif deductible == 2500:
        netloss = max(net_loss - num_of_claims * 22500, 0)
    elif deductible == 2000:
        netloss = max(net_loss - num_of_claims * 23000, 0)     
    elif deductible == 1500:
        netloss = max(net_loss - num_of_claims * 23500, 0)     
    elif deductible == 1000:
        netloss = max(net_loss - num_of_claims * 24000, 0)    
    elif deductible == 750:
        netloss = max(net_loss - num_of_claims * 24250, 0)
    elif deductible == 500:
        netloss = max(net_loss - num_of_claims * 24500, 0)
    elif deductible == 250: 
        netloss = max(net_loss - num_of_claims * 24750, 0)
    elif deductible == 200:
        netloss = max(net_loss - num_of_claims * 24800, 0)
    elif deductible == 100:
        netloss = max(net_loss - num_of_claims * 24900,0)
    else: 
        netloss = 0
          
    return netloss
    

df_water['net_loss_10000_to_25000'] = df_water.apply(lambda row: step14(row), axis=1)   

df_water.columns    

list_of_values=[100,200,250,500,750,1000,1500,2000,2500,3000,4000,5000, 7500, 10000]
display(df_water[df_water['Deductible'].isin(list_of_values)][['net_loss_10000_to_25000','Deductible','net_loss','calc_claim']])

df_10000_to_25000 =df_water[df_water['Deductible'].isin(list_of_values)].groupby(['CovA_cat']).agg({'net_loss_10000_to_25000':'sum','net_loss':'sum'}).reset_index()

display(df_10000_to_25000)
df_10000_to_25000['ler']= 1- df_10000_to_25000['net_loss_10000_to_25000']/df_10000_to_25000['net_loss']

df_10000_to_25000_T = df_10000_to_25000.T

df_10000_to_25000_T = df_10000_to_25000_T.fillna(0)
display(df_10000_to_25000_T)

df_10000_to_25000_T=df_10000_to_25000_T.drop(index='CovA_cat')

combine=pd.read_pickle('D:\Sang\combine_elr.pickle')

combine=pd.concat([combine,df_10000_to_25000_T],axis=0)

combine.to_pickle('D:\Sang\combine_elr.pickle')

combine.to_csv('D:\Sang\combine_elr.csv')
