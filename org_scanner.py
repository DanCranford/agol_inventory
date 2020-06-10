#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from agol_inventory import *


# In[ ]:



url = input('ArcGIS Org URL: ')
username = input('Username: ')
session_gis = arcgis.GIS(url, username, set_active=False, verify_cert = False)
just_me = False
output_db = input('Output DB Path: ')
thread_count = 15

# In[ ]:


inventory_dict = set_up_dict_lists()


# In[ ]:

print('Scanning Groups...')
group_scan(session_gis, inventory_dict, thread_count)


# In[ ]:

print('Scanning Users...')
folder_dict = user_scan(session_gis, inventory_dict, thread_count)


# In[ ]:


print('Scanning Items...')
item_scan(session_gis, inventory_dict, folder_dict, thread_count, just_me)


# In[ ]:


output_to_sqlite(inventory_dict, output_db)


# In[ ]:




