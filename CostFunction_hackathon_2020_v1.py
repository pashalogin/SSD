#!/usr/bin/env python
# coding: utf-8

# In[1]:


###################################################
#### Authors: Abdullah Al Khaled, Riad Mohamed ####
###################################################


# In[2]:


from haversine import haversine, Unit
import pandas as pd
import numpy as np


# In[3]:


#List of DFs
#path = r"C:\Users\abdullah.alkhaled\Desktop\SCM Project\same-day-optimization\Offer_Management"
#zip_info = pd.read_csv(path + "\zip_info_STATIC_v3.csv", sep=",")
#zip_slot_info = pd.read_csv(path + "\zip_slot_info_STATIC_v3.csv", sep=",")
#slot_maxZipCount = pd.read_csv(path + "\slot_maxZipCount_STATIC_v3.csv", sep=",")

#slot_capacity = pd.read_csv(path + "\slot_capacity_DYNAMIC_full_v3.csv", sep=",")
#zip_slot_sim = pd.read_csv(path + "\zip_slot_sim_DYNAMIC_v3.csv", sep=",")


#def prepare_DF (market,day_of_wk): #market ='Nashville TN' day_of_wk = 0 
 #   global zip_info, zip_slot_info,slot_maxZipCount,slot_capacity,zip_slot_sim
 #   zip_info = zip_info[zip_info.market == market]
  #  zip_slot_info = zip_slot_info[zip_slot_info.market == market]
   # slot_maxZipCount = slot_maxZipCount[slot_maxZipCount.market == market]
   # slot_sim = slot_capacity[(slot_capacity.market == market)&(slot_capacity.day_of_wk == day_of_wk)]
   # zip_slot_sim = zip_slot_sim[zip_slot_sim.market == market]
   # return zip_info,zip_slot_info,slot_maxZipCount,slot_sim,zip_slot_sim    


# In[4]:


#4.1.0: Stem distance -- FSL location needed
def d_0(zipcode,slot,zip_info,zip_slot_sim): 
    zip_open = 0 
    try:
        for i in zip_info[zip_info.zipcode == str(zipcode)].adj_zip_30.values[0]:
            zip_open += zip_slot_sim[(zip_slot_sim.zipcode == str(i)) & (zip_slot_sim.slot == int(slot))].served_flag.values[0]
    except:
        zip_open = 0
    if zip_open >= 1:
        d_0 = 0
    else:
        d_0 = zip_info[zip_info.zipcode == str(zipcode)].stem_distance.values[0]
    return d_0


# In[5]:


#4.1.1: Distance Between Stops Within a Zip Code (i) within a Time Slot (s)
def d_is_n(zipcode,zip_info):
    d_is_n =zip_info[zip_info.zipcode == str(zipcode)].radius*2
    return d_is_n.values[0]


# In[6]:


#4.1.2: Distance Between Stops Within a Zip Code (i) within a Time Slot (s)
def d_is_z(zipcode,slot,zip_info,zip_slot_sim):
    d_z_avg = 2*zip_info[zip_info.zipcode == str(zipcode)].radius.values[0]
    k_zipcode = 1 #adjust the k value
    tau = 0
    area = 0.0
    try:
        for i in zip_info[zip_info.zipcode == str(zipcode)].adj_zip_30.values[0]:
            tau += min(1, zip_info[zip_info.zipcode==str(i)].AvgClaimsPerDay.values[0])*zip_slot_sim[(zip_slot_sim.zipcode == str(i)) & (zip_slot_sim.slot == int(slot))].served_flag.values[0]
            area += zip_info[zip_info.zipcode == str(i)].area.values[0]
    except:
        area = 1
        tau = 1
    delta = (tau/area)*1.0
    d_is_z = min (d_z_avg, k_zipcode/np.sqrt(delta))
    return d_is_z
    


# In[7]:


#4.1.3
def d_is_t(zipcode,slot,zip_info,zip_slot_sim):
    d_is_z_pre = 0
    d_is_z_post = 0
    
    if slot>1300:
        if zip_slot_sim[(zip_slot_sim.zipcode == str(zipcode)) & (zip_slot_sim.slot == int(slot)-200)].served_flag.values[0]:
            d_is_z_pre = d_is_n(zipcode,zip_info)
        else:
            d_is_z_pre = d_is_z(zipcode,slot-200,zip_info,zip_slot_sim)
    else:
        d_is_z_pre = d_is_n(zipcode,zip_info)
    if slot<1900:
        if zip_slot_sim[(zip_slot_sim.zipcode == str(zipcode)) & (zip_slot_sim.slot == int(slot)+200)].served_flag.values[0]:
            d_is_z_post = d_is_n(zipcode,zip_info)
        else:
            d_is_z_post= d_is_z(zipcode,slot+200,zip_info,zip_slot_sim)
    else:
        d_is_z_post = d_is_n(zipcode,zip_info)
        
    d_is_t = (d_is_z_pre+d_is_z_post)/2

    return d_is_t


# In[8]:


#4.1.4
def n_is_n(zipcode,slot,zip_slot_info):
    AvgClaimsPerDay = zip_slot_info[(zip_slot_info.zipcode == str(zipcode))&(zip_slot_info.scheduled_service_time == int(slot))].AvgClaimsPerDay.values[0] if len(zip_slot_info[(zip_slot_info.zipcode == str(zipcode))&(zip_slot_info.scheduled_service_time == int(slot))].AvgClaimsPerDay) > 1 else 1
    n_is_n =min(3, AvgClaimsPerDay)
    return n_is_n


# In[9]:


#4.1.5
def n_is_z(zipcode,slot,zip_info,zip_slot_sim, slot_maxZipCount):
    n_open = 0
    try:
        for i in zip_info[zip_info.zipcode == str(zipcode)].adj_zip_30.values[0]:
            n_open += (zip_slot_sim[(zip_slot_sim.zipcode == str(i)) & (zip_slot_sim.slot == int(slot))].served_flag.values[0])
        n_open= max(1,n_open)
    except:
        n_open= 1
    v_z = slot_maxZipCount[slot_maxZipCount.scheduled_service_time ==int(slot)].maxZipCount.values[0] #historical value
    n_is_z = min(n_open, v_z)
    return n_is_z


# In[10]:


#4.1.6
def n_is_t(zipcode,slot,expert_type):#expert_type = "FT"/"PT"
    if expert_type=="FT":
        n_is_t = 4
    else:
        n_is_t = 2
    
    return n_is_t


# In[11]:


def n_is(zipcode,slot,expert_type,zip_info,zip_slot_info,slot_maxZipCount, zip_slot_sim):
    n_is_t_value = n_is_t(zipcode,slot,expert_type)
    n_is_z_value = n_is_z(zipcode,slot,zip_info,zip_slot_sim, slot_maxZipCount)
    n_is_n_value = n_is_n(zipcode,slot,zip_slot_info)
    n_is =(n_is_t_value)*(n_is_z_value)*(n_is_n_value)
    return n_is


# In[12]:


#DPO value

def DPO(zipcode,slot,expert_type,zip_info,zip_slot_info,slot_maxZipCount, zip_slot_sim):
    
    n_is_t_value = n_is_t(zipcode,slot,expert_type)
    n_is_z_value = n_is_z(zipcode,slot,zip_info,zip_slot_sim, slot_maxZipCount)
    n_is_n_value = n_is_n(zipcode,slot,zip_slot_info)
    n_is_value = n_is(zipcode,slot,expert_type,zip_info,zip_slot_info,slot_maxZipCount, zip_slot_sim)
    
    d_0_value = d_0(zipcode,slot,zip_info,zip_slot_sim)
    d_is_t_value = d_is_t(zipcode,slot,zip_info,zip_slot_sim)
    d_is_z_value = d_is_z(zipcode,slot,zip_info,zip_slot_sim)
    d_is_n_value = d_is_n(zipcode,zip_info)
    
    
    term1 = (n_is_n_value-1)*d_is_n_value
    term2 = (n_is_z_value-1)*(d_is_z_value/n_is_z_value)
    term3 = (n_is_t_value-1)*(d_is_t_value/n_is_t_value)
    
    DPO_value =(n_is_t_value/n_is_value)*(term3+n_is_z_value*(term2+term1)+d_0_value)
    
    return DPO_value


# In[13]:


#Cost function dictionary
def slotCosts(zipcode,slot,expert_type,zip_info,zip_slot_info,slot_maxZipCount, zip_slot_sim):#round(number, number of digits)
    if expert_type == "FT":
        DPO_1 = round(DPO(zipcode,slot,expert_type,zip_info,zip_slot_info,slot_maxZipCount, zip_slot_sim), 2)
        DPO_2 = round(DPO(zipcode,slot+200,expert_type,zip_info,zip_slot_info,slot_maxZipCount, zip_slot_sim), 2)
    else:
        DPO_1 = np.nan
        DPO_2 = np.nan
    DPO_3 = round(DPO(zipcode,slot+400,expert_type,zip_info,zip_slot_info,slot_maxZipCount, zip_slot_sim), 2)
    DPO_4 = round(DPO(zipcode,slot+600,expert_type,zip_info,zip_slot_info,slot_maxZipCount, zip_slot_sim), 2)
    Dict = dict({'1300': DPO_1, '1500': DPO_2, '1700': DPO_3, '1900': DPO_4}) 
    return Dict


# In[14]:


#def CostFunction(market,day_of_wk,zipcode,slot,expert_type):
    #global prepare_DF, d_0, 
 #   zip_info,zip_slot_info,slot_maxZipCount,slot_sim,zip_slot_sim = prepare_DF (market,day_of_wk)

  #  zip_info.zipcode = zip_info.zipcode.astype(str)
   # zip_slot_info.zipcode = zip_slot_info.zipcode.astype(str)
   # zip_slot_sim.zipcode = zip_slot_sim.zipcode.astype(str)
    
   # zip_slot_info.scheduled_service_time = zip_slot_info.scheduled_service_time.astype('int64')
   # zip_slot_sim.slot = zip_slot_sim.slot.astype('int64')
   # zip_info['adj_zip_30'] = zip_info['adj_zip_30'].apply(lambda x: x.replace("[", "").replace("]", "").replace("'", "").replace("\n", "").split(" "))
    
    #cost_dictionary = slotCosts(zipcode,slot,expert_type,zip_info,zip_slot_info,slot_maxZipCount, zip_slot_sim)
    
    #return cost_dictionary


# In[15]:


#costs = CostFunction('Nashville TN',0,35620,1300,"FT")
#costs


# In[ ]:




