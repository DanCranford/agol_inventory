#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from item_inventory import *


# In[ ]:



url = input('ArcGIS Org URL: ')
username = input('Username: ')
session_gis = arcgis.GIS(url, username)


# In[ ]:


inventory_dict = set_up_dict_lists()


# In[ ]:


group_scan(session_gis, inventory_dict, 15)


# In[ ]:


folder_dict = user_scan(session_gis, inventory_dict, 15)


# In[ ]:



item_scan(session_gis, inventory_dict, folder_dict, 15)


# In[ ]:


output_to_sqlite(inventory_dict, r'./inventory.sqlite')


# In[ ]:




