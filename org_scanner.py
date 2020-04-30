#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from item_inventory import *


# In[ ]:



url = input('ArcGIS Org URL: ')
username = input('Username: ')
session_gis = arcgis.GIS(url, username)
just_me = False


# In[ ]:


inventory_dict = set_up_dict_lists()


# In[ ]:

print('Scanning Groups...')
group_scan(session_gis, inventory_dict, 15)


# In[ ]:

print('Scanning Users...')
folder_dict = user_scan(session_gis, inventory_dict, 15)


# In[ ]:


print('Scanning Items...')
item_scan(session_gis, inventory_dict, folder_dict, 15, just_me)


# In[ ]:

print(())
output_to_sqlite(inventory_dict, r'./inventory.sqlite')


# In[ ]:




