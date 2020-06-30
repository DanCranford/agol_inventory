#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from agol_inventory import *


# In[ ]:



url = 'https://gtvm.maps.arcgis.com' #input('ArcGIS Org URL: ')
username = 'GTVMAPS' #input('Username: ')
session_gis = arcgis.GIS(url, username, set_active=False, verify_cert = False)
depth = 'extended'
output_db = './gtvm.sqlite' #input('Output DB Path: ')
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
item_scan(session_gis, inventory_dict, folder_dict, thread_count, depth)


# In[ ]:


# output_to_sqlite(inventory_dict, output_db)


# In[ ]:




