#!/usr/bin/env python
# coding: utf-8

# In[86]:


import numpy as np
import pandas as pd


# In[ ]:


data = pd.read_csv("inputF.csv",encoding="UTF'8")
data['FECHA']=data['FECHA'].str.replace("00:00","")
ti=['Á','É' ,'Ó' ,'Ú' ,'Í']
re=['A','E','O','U','I']
for i in range(5):
    data['TIPO ACCIDENTE']=data['TIPO ACCIDENTE'].str.replace(ti[i],re[i])
    data['LUGAR ACCIDENTE']=data['LUGAR ACCIDENTE'].str.replace(ti[i],re[i])
data=data.drop([248016])
data.to_csv("inputFinal.csv",index=False)


# In[ ]:





# In[ ]:




