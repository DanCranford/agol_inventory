













import arcgis
# import arcpy
import time
import pandas
import sqlite3
import os
from threading import Thread
from queue import Queue
import sys
from IPython.display import clear_output


def update_progress(progress):
    bar_length = 20
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
    if progress < 0:
        progress = 0
    if progress >= 1:
        progress = 1

    block = int(round(bar_length * progress))

    clear_output(wait = True)
    text = "Progress: [{0}] {1:.1f}%".format( "#" * block + "-" * (bar_length - block), progress * 100)
    print(text)


def map_layer_editable(op_lyr):
    editing = False
    try:
        for field in op_lyr['popupInfo']['fieldInfos']:
            if field['isEditable']:
                editing = True
                break
    except:
        editing = False
    return editing


def print_message(message):
    print(message)
    arcpy.AddMessage(message)

    
def online_to_pst_time(time_value):
    pst_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime((time_value / 1000.0) - 25200))
    return pst_time


def dlist_to_sqlite(dlist, connection, table_name, **kwargs):
    df = pandas.DataFrame(dlist[1:], columns=dlist[0])
    df.index.name = 'OID'
    df.to_sql(table_name, connection, if_exists='replace', **kwargs)
    connection.commit()

    
def item_grab(queue, dict_lists, folder_dict):
    # common attributes
    while not queue.empty():
        work = queue.get()
        item_desc = work[1]
        try:
            name = item_desc.title
            # print(name)
            try:
                folder_desc = folder_dict[item_desc.ownerFolder]
            except:
                folder_desc = None
            shared = item_desc.access
            owner_desc = item_desc.owner
            created = online_to_pst_time(item_desc.created)
            modified = online_to_pst_time(item_desc.modified)
            itemid = item_desc.id
            size = item_desc.size
            content_status = item_desc.content_status
            if item_desc.categories:
                for category in item_desc.categories:
                    dict_lists['CATEGORIES'].append([itemid, category])
            for tag in item_desc.tags:
                dict_lists['TAGS'].append([itemid, tag])
            try:
                shared_with = item_desc.shared_with
                everyone = shared_with['everyone']
                org = shared_with['org']
                groups = len(shared_with['groups'])
            except:
                shared_with = item_desc._portal.con.get('content/users/{}/items/{}'.format(item_desc._user_id,
                                                                                           item_desc.id))['sharing']
                if shared_with['access'] == 'public':
                    everyone = True
                    org = True
                elif shared_with['access'] == 'org':
                    everyone = False
                    org = True
                else:
                    everyone = False
                    org = False
                groups = len(shared_with['groups'])

            if item_desc.type == 'Feature Service':
                # add to list of feature services
                if 'View Service' in item_desc.typeKeywords:
                    is_view = True
                    source_item_id = item_desc.related_items('Service2Service', 'reverse')[0].id
                    source_item_name = item_desc.related_items('Service2Service', 'reverse')[0].title
                else:
                    is_view = False
                    source_item_id = None
                    source_item_name = None
                dict_lists['FEATURE_SERVICES'].append(
                    [item_desc.type, name, folder_desc, shared, everyone, org, groups, owner_desc, created, modified,
                     itemid, is_view, source_item_id, source_item_name, size, content_status])
            elif item_desc.type == 'Web Map':
                # add to list of mapsg
                dict_lists['WEB_MAPS'].append(
                    [item_desc.type, name, folder_desc, shared, everyone, org, groups, owner_desc, created, modified,
                     itemid, size, content_status])
                webmap = arcgis.mapping.WebMap(item_desc)
                for layer in webmap.layers:
                    try:
                        layername = layer.title
                    except KeyError:
                        layername = 'N/A'
                    try:
                        layer_item_id = layer.itemId
                    except:
                        layer_item_id = 'N/A'
                    try:
                        layerfilter = layer['layerDefinition']['definitionExpression']
                    except KeyError:
                        layerfilter = None
                    try:
                        layerurl = layer.url
                    except:
                        layerurl = None
                    editable = map_layer_editable(layer)
                    # add to Map/Layer Join Table
                    dict_lists['MAP_FS_REL'].append([name, itemid, layername, editable, layerfilter, layer_item_id, layerurl])
                for bm in webmap.basemap['baseMapLayers']:
                    try:
                        layername = "Basemap - {}".format(bm['title'])
                    except KeyError:
                        layername = "Basemap - {}".format(webmap.basemap['title'])
                    try:
                        layerurl = bm['url']
                    except KeyError:
                        layerurl = 'N/A'
                    dict_lists['MAP_FS_REL'].append(
                        [name, itemid, layername, False, None, None, layerurl])

            elif item_desc.type == 'Web Mapping Application':
                appdata = item_desc.get_data()
                try:
                    apptoMapID = appdata['map']['itemId']
                except:
                    apptoMapID = None
                del (appdata)
                dict_lists['WEB_APPS'].append(
                    [item_desc.type, name, folder_desc, shared, everyone, org, groups, owner_desc, created, modified,
                     itemid, apptoMapID, size, content_status])
            else:
                dict_lists['OTHER_ITEMS'].append(
                    [item_desc.type, name, folder_desc, shared, everyone, org, groups, owner_desc, created, modified,
                     itemid, size, content_status])
        except Exception as e:
            print(item_desc)
            errord.append(item_desc)
            print_message('something went wrong')
            print_message(e)
        queue.task_done()
    return True


def item_scan(gis_object, dict_lists, folder_dict, num_threads, justme=False):
    try:
        if justme:
            item_list = gis_object.content.advanced_search('owner:{}'.format(username), max_items=9999)['results']
        else:
            item_list = gis_object.content.advanced_search('accountid: {}'.format(GIS.properties.get('id')), 
                                                               max_items = 9999)['results']
    except:
        if justme:
            item_list = gis_object.content.search('owner:{}'.format(username), max_items=9999)
        else:
            item_list = gis_object.content.search('*', max_items = 9999)
    
    q = Queue(maxsize=0)

    for i, item in enumerate(item_list):
        q.put((i, item))
        
    if len(item_list) < num_threads:
        num_threads = len(item_list)

    for i in range(num_threads):
        worker = Thread(target=item_grab, args=(q, dict_lists, folder_dict))
        worker.setDaemon(True)
        worker.start()

    q.join()
    
    
def set_up_dict_lists():
    return {
        'FEATURE_SERVICES': [
            ['ITEM_TYPE', 'ITEM_NAME', 'FOLDER', 'SHARED', 'EVERYONE', 'ORG', 'GROUPS', 'OWNER', 'CREATEDATE', 'MODATE',
             'ITEM_ID', 'ISVIEW', 'SOURCE_ITEM_ID', 'SOURCE_ITEM_NAME', 'SIZE', 'CONTENT_STATUS']],
        'GROUP_MEMBERSHIP': [
            ['GROUP_NAME', 'MEMBER', 'MEMBERTYPE']],
        'GROUPS': [
            ['GROUP_NAME', 'OWNER', 'ADMINCOUNT', 'MEMBERCOUNT', 'ID', 'CREATEDATE', 'ITEMS']],
        'MAP_FS_REL': [
            ['WEBMAP_TITLE', 'WEBMAP_ID', 'LAYER_NAME', 'EDITABLE', 'LAYER_FILTER', 'LAYER_ITEM_ID', 'LAYER_URL']],
        'WEB_APPS': [
            ['ITEM_TYPE', 'ITEM_NAME', 'FOLDER', 'SHARED', 'EVERYONE', 'ORG', 'GROUPS', 'OWNER', 'CREATEDATE', 'MODATE',
             'ITEM_ID', 'WEBMAP_ID', 'SIZE', 'CONTENT_STATUS']],
        'WEB_MAPS': [
            ['ITEM_TYPE', 'ITEM_NAME', 'FOLDER', 'SHARED', 'EVERYONE', 'ORG', 'GROUPS', 'OWNER', 'CREATEDATE', 'MODATE',
             'ITEM_ID', 'SIZE', 'CONTENT_STATUS']],
        'OTHER_ITEMS': [
            ['ITEM_TYPE', 'ITEM_NAME', 'FOLDER', 'SHARED', 'EVERYONE', 'ORG', 'GROUPS', 'OWNER', 'CREATEDATE', 'MODATE',
             'ITEM_ID', 'SIZE', 'CONTENT_STATUS']],
        'SHARING': [
            ['ITEM_ID', 'GROUP_NAME']],
        'USERS': [
            ['USERNAME', 'FIRSTNAME', 'LASTNAME', 'LEVEL', 'ROLE', 'CREATED', 'LAST_LOGIN', 'DESCRIPTION']],
        'CATEGORIES': [
            ['ITEM_ID', 'CATEGORY']],
        'TAGS': [
            ['ITEM_ID', 'TAG']]
            }


def group_grab(queue, dict_lists):
    while not queue.empty():
        work = queue.get()
        group = work[1]
        groupid = group.id
        title = group.title
        members = group.get_members()
        owner = members['owner']
        dict_lists['GROUP_MEMBERSHIP'].append([title, owner, 'OWNER'])
        admins = members['admins']
        for admin in admins:
            # add admin to groupmember list
            dict_lists['GROUP_MEMBERSHIP'].append([title, admin, 'ADMIN'])
        users = members['users']
        created = online_to_pst_time(group.created)
        content = group.content()
        for item in content:
            dict_lists['SHARING'].append([item.id, group.title])
        for user in users:
            # add user to groupmember list
            dict_lists['GROUP_MEMBERSHIP'].append([title, user, 'USER'])
        dict_lists['GROUPS'].append([title, owner, len(admins), len(users), groupid, created, len(content)])
        queue.task_done()
    return True
    
    
def group_scan(gis_object, dict_lists, num_threads):
    groups = gis_object.groups.search()
    groupids = [group.id for group in groups]
    for group in gis_object.users.me.groups:
        if group.id not in groupids:
            groups.append(group)
    
    q = Queue(maxsize=0)

    for i, group in enumerate(groups):
        q.put((i, group))
        
    if num_threads < len(group):
        num_threads = len(group)    
    
    for i in range(num_threads):
        worker = Thread(target=group_grab, args=(q, dict_lists))
        worker.setDaemon(True)
        worker.start()

    q.join()   
    
    
    
def user_grab(queue, dict_lists, folder_dict):
    while not queue.empty():
        work = queue.get()
        user = work[1]
        username = user.username
        created = online_to_pst_time(user.created)
        firstname = user.firstName
        lastname = user.lastName
        level = user.level
        roleID = user.roleId
        description = user.description
        last_login = online_to_pst_time(user.lastLogin)
        dict_lists['USERS'].append([username, firstname, lastname, level, roleID, created, last_login, description])
        for folder in user.folders:
            folder_dict[folder['id']] = folder['title']
        queue.task_done()
    return True


def user_scan(gis_object, dict_lists, num_threads):
    users = gis_object.users.search()
    folder_dict = {None: None}
    
    q = Queue(maxsize=0)

    for i, user in enumerate(users):
        q.put((i, user))
        
    if num_threads < len(users):
        num_threads = len(users)
    
    for i in range(num_threads):
        worker = Thread(target=user_grab, args=(q, dict_lists, folder_dict))
        worker.setDaemon(True)
        worker.start()

    q.join()   
    return folder_dict
    

def output_to_sqlite(dict_lists, sqlite_path):
    sql_views = [
        """CREATE VIEW APPS_TO_MAPS AS
        SELECT *
          FROM WEB_APPS WA
               LEFT JOIN
               WEB_MAPS WM ON WA.WEBMAP_ID = WM.ITEM_ID;"""
        ,
        """CREATE VIEW BROKEN_LAYERS AS
        SELECT REL.WEBMAP_TITLE,
               REL.LAYER_NAME,
               FS.ITEM_NAME AS FS_NAME,
               WM.SHARED AS WM_SHARED,
               FS.SHARED AS FS_SHARED
          FROM MAP_FS_REL REL
               LEFT JOIN
               WEB_MAPS WM ON REL.WEBMAP_ID = WM.ITEM_ID
               LEFT JOIN
               FEATURE_SERVICES FS ON REL.LAYER_ITEM_ID = FS.ITEM_ID
         WHERE WM.SHARED <> FS.SHARED AND 
               FS.SHARED = 'Private';"""
        ,
        """CREATE VIEW ALL_ITEMS AS
        SELECT ITEM_TYPE,
               ITEM_NAME,
               FOLDER,
               SHARED,
               EVERYONE,
               ORG,
               GROUPS,
               OWNER,
               CREATEDATE,
               MODATE,
               ITEM_ID,
               SIZE,
               CONTENT_STATUS
          FROM FEATURE_SERVICES
        UNION ALL
        SELECT ITEM_TYPE,
               ITEM_NAME,
               FOLDER,
               SHARED,
               EVERYONE,
               ORG,
               GROUPS,
               OWNER,
               CREATEDATE,
               MODATE,
               ITEM_ID,
               SIZE,
               CONTENT_STATUS
          FROM WEB_MAPS
        UNION ALL
        SELECT ITEM_TYPE,
               ITEM_NAME,
               FOLDER,
               SHARED,
               EVERYONE,
               ORG,
               GROUPS,
               OWNER,
               CREATEDATE,
               MODATE,
               ITEM_ID,
               SIZE,
               CONTENT_STATUS
          FROM WEB_APPS
        UNION ALL
        SELECT ITEM_TYPE,
               ITEM_NAME,
               FOLDER,
               SHARED,
               EVERYONE,
               ORG,
               GROUPS,
               OWNER,
               CREATEDATE,
               MODATE,
               ITEM_ID,
               SIZE,
               CONTENT_STATUS
          FROM OTHER_ITEMS;
    """
        ,
        """CREATE VIEW SHARED_EVERYONE AS
        SELECT *
          FROM ALL_ITEMS
         WHERE EVERYONE = 1
         ORDER BY OWNER;"""
        ,
        """CREATE VIEW GROUPS_ZEROMEMBERS AS
        SELECT *
          FROM GROUPS
         WHERE MEMBERCOUNT = 0;"""
        ,
        """CREATE VIEW USERS_NEVERLOGIN AS
        SELECT *
          FROM USERS
         WHERE LAST_LOGIN = -1;"""
        ,
        """CREATE VIEW USERS_VIEW AS
        SELECT USERNAME,
               FIRSTNAME,
               LASTNAME,
               LEVEL,
               ROLE,
               CREATED,
               LAST_LOGIN,
               ROUND(JULIANDAY({}, 'unixepoch','localtime') - JULIANDAY(LAST_LOGIN),-2) AS DAYS_STAGNANT,
               DESCRIPTION
          FROM USERS;""".format(time.time())
        ,
        """CREATE VIEW OTO_FS_MAP AS
        SELECT LAYER_ITEM_ID,
               WEBMAP_ID
          FROM MAP_FS_REL
         GROUP BY LAYER_ITEM_ID,
                  WEBMAP_ID;"""
        ,
        """CREATE VIEW SHARING_GROUPS AS
        SELECT
          ITEM_ID,
        COUNT(*) AS GROUP_COUNT,
        GROUP_CONCAT(GROUP_NAME) AS GROUPS
         FROM SHARING
         GROUP BY ITEM_ID"""
        ]

    conn = sqlite3.connect(sqlite_path)
    for key in dict_lists:
        dlist_to_sqlite(dict_lists[key], conn, key)

    cursor = sqlite3.Cursor(conn)
    for statement in sql_views:
        cursor.execute(statement)
    conn.close()


# print_message('Starting Group Scan...')
# groups = GIS.groups.search()
# for group in GIS.users.me.groups:
#     if group not in groups:
#         groups.append(group)

# for group in groups:
#     groupid = group.id
#     title = group.title
#     members = group.get_members()
#     owner = members['owner']
#     data_inventory['GROUP_MEMBERSHIP'].append([title, owner, 'OWNER'])
#     admins = members['admins']
#     for admin in admins:
#         # add admin to groupmember list
#         data_inventory['GROUP_MEMBERSHIP'].append([title, admin, 'ADMIN'])
#     users = members['users']
#     created = online_to_pst_time(group.created)
#     content = group.content()
#     for item in content:
#         data_inventory['SHARING'].append([item.id, group.title])
#     for user in users:
#         # add user to groupmember list
#         data_inventory['GROUP_MEMBERSHIP'].append([title, user, 'USER'])
#     data_inventory['GROUPS'].append([title, owner, len(admins), len(users), groupid, created, len(content)])




# # arcgis org administrator info
# org = input('org: ')
# org_url = input('org url: ')
# username = input('username: ')
# password = input('password: ')
# justme = input('Just This User? Y/N: ')
# folder = input('Destination DB Folder: ')
# num_threads = int(input('Thread Count: '))

# # org = 'wevm'
# # org_url = 'https://wevm.maps.arcgis.com'
# # username = 'dpcn@pge'
# # password = ''
# # justme = 'N'
# # folder = r'\\rcshare01-nas\EncroachmentManagement\9_ActiveWorkspace\GIS_PM\AGOL_Org_Inventory'
# # num_threads = 5
# exclude_items = []



# # output sqlite database
# todate = strftime("%Y%m%d")
# start_time = time.time()
# if not os.path.exists(os.path.join(folder, org)):
#     os.mkdir(os.path.join(folder, org))
# db = os.path.join(folder, org, "{}_{}.sqlite".format(org, todate))

# GIS = arcgis.GIS(org_url, username, password)
# data_inventory = {
#     'FEATURE_SERVICES': [
#         ['ITEM_TYPE', 'ITEM_NAME', 'FOLDER', 'SHARED', 'EVERYONE', 'ORG', 'GROUPS', 'OWNER', 'CREATEDATE', 'MODATE',
#          'ITEM_ID', 'ISVIEW', 'SOURCE_ITEM_ID', 'SOURCE_ITEM_NAME', 'SIZE', 'CONTENT_STATUS']],
#     'GROUP_MEMBERSHIP': [
#         ['GROUP_NAME', 'MEMBER', 'MEMBERTYPE']],
#     'GROUPS': [
#         ['GROUP_NAME', 'OWNER', 'ADMINCOUNT', 'MEMBERCOUNT', 'ID', 'CREATEDATE', 'ITEMS']],
#     'MAP_FS_REL': [
#         ['WEBMAP_TITLE', 'WEBMAP_ID', 'LAYER_NAME', 'LAYER_FILTER', 'LAYER_ITEM_ID', 'LAYER_URL']],
#     'WEB_APPS': [
#         ['ITEM_TYPE', 'ITEM_NAME', 'FOLDER', 'SHARED', 'EVERYONE', 'ORG', 'GROUPS', 'OWNER', 'CREATEDATE', 'MODATE',
#          'ITEM_ID', 'WEBMAP_ID', 'SIZE', 'CONTENT_STATUS']],
#     'WEB_MAPS': [
#         ['ITEM_TYPE', 'ITEM_NAME', 'FOLDER', 'SHARED', 'EVERYONE', 'ORG', 'GROUPS', 'OWNER', 'CREATEDATE', 'MODATE',
#          'ITEM_ID', 'SIZE', 'CONTENT_STATUS']],
#     'OTHER_ITEMS': [
#         ['ITEM_TYPE', 'ITEM_NAME', 'FOLDER', 'SHARED', 'EVERYONE', 'ORG', 'GROUPS', 'OWNER', 'CREATEDATE', 'MODATE',
#          'ITEM_ID', 'SIZE', 'CONTENT_STATUS']],
#     'SHARING': [
#         ['ITEM_ID', 'GROUP_NAME']],
#     'USERS': [
#         ['USERNAME', 'FIRSTNAME', 'LASTNAME', 'LEVEL', 'ROLE', 'CREATED', 'LAST_LOGIN', 'DESCRIPTION']],
#     'CATEGORIES': [
#         ['ITEM_ID', 'CATEGORY']],
#     'TAGS': [
#         ['ITEM_ID', 'TAG']]
# }

# folder_dict = {None: None}


# print_message('Starting Group Scan...')
# groups = GIS.groups.search()
# for group in GIS.users.me.groups:
#     if group not in groups:
#         groups.append(group)

# for group in groups:
#     groupid = group.id
#     title = group.title
#     members = group.get_members()
#     owner = members['owner']
#     data_inventory['GROUP_MEMBERSHIP'].append([title, owner, 'OWNER'])
#     admins = members['admins']
#     for admin in admins:
#         # add admin to groupmember list
#         data_inventory['GROUP_MEMBERSHIP'].append([title, admin, 'ADMIN'])
#     users = members['users']
#     created = online_to_pst_time(group.created)
#     content = group.content()
#     for item in content:
#         data_inventory['SHARING'].append([item.id, group.title])
#     for user in users:
#         # add user to groupmember list
#         data_inventory['GROUP_MEMBERSHIP'].append([title, user, 'USER'])
#     data_inventory['GROUPS'].append([title, owner, len(admins), len(users), groupid, created, len(content)])

# # for group in GIS.users.me.groups:
# #     for item in group.content():
# #         data_inventory['SHARING'].append([item.id, group.title])

# print_message('Starting User Scan...')
# users = GIS.users.search()
# for user in users:
#     username = user.username
#     created = online_to_pst_time(user.created)
#     try:
#         firstname = user.firstName
#         lastname = user.lastName
#     except:
#         firstname = user.fullName
#         lastname = 'N/A'
#     level = user.level
#     roleID = user.roleId
#     description = user.description
#     last_login = online_to_pst_time(user.lastLogin)
#     data_inventory['USERS'].append([username, firstname, lastname, level, roleID, created, last_login, description])
#     try:
#         for folder in user.folders:
#             folder_dict[folder['id']] = folder['title']
#     except:
#         None

# print_message('Starting Item Scan...')
# errord = []
# try:
#     if justme == 'Y':
#         dupe_itemList = GIS.content.advanced_search('owner:{}'.format(username), max_items=9999)['results']
#     else:
#         dupe_itemList = GIS.content.advanced_search('accountid: {}'.format(GIS.properties.get('id')), max_items = 9999)['results']
# except:
#     if justme == 'Y':
#         dupe_itemList = GIS.content.search('owner:{}'.format(username), max_items=9999)
#     else:
#         dupe_itemList = GIS.content.search('*', max_items = 9999)

# # REMOVE DUPES FROM ITEM LIST
# # uniques = []
# # dupes = []
# # itemList = []
# # for item in dupe_itemList:
# #     if item.id in uniques:
# #         dupes.append(item.id)
# #     else:
# #         itemList.append(item)
# #         uniques.append(item.id)

# itemList = [item for item in dupe_itemList if item.title != 'ETGIS']

# print_message('\tTotal Items: ' + str(len(itemList)))
# start_time = time.time()

# def item_descrip(queue, dict_lists):
#     # common attributes
#     while not queue.empty():
#         work = queue.get()
#         item_desc = work[1]
#         if item.id not in exclude_items:
#             try:
#                 name = item_desc.title
#                 # print(name)
#                 try:
#                     folder_desc = folder_dict[item_desc.ownerFolder]
#                 except:
#                     folder_desc = ''
#                 shared = item_desc.access
#                 owner_desc = item_desc.owner
#                 created = online_to_pst_time(item_desc.created)
#                 modified = online_to_pst_time(item_desc.modified)
#                 itemid = item_desc.id
#                 size = item_desc.size
#                 content_status = item_desc.content_status
#                 if item_desc.categories:
#                     for category in item_desc.categories:
#                         dict_lists['CATEGORIES'].append([itemid, category])
#                 for tag in item_desc.tags:
#                     dict_lists['TAGS'].append([itemid, tag])
#                 try:
#                     shared_with = item_desc.shared_with
#                     everyone = shared_with['everyone']
#                     org = shared_with['org']
#                     groups = len(shared_with['groups'])
#                 except:
#                     shared_with = item_desc._portal.con.get('content/users/{}/items/{}'.format(item_desc._user_id,
#                                                                                                item_desc.id))['sharing']
#                     if shared_with['access'] == 'public':
#                         everyone = True
#                         org = True
#                     elif shared_with['access'] == 'org':
#                         everyone = False
#                         org = True
#                     else:
#                         everyone = False
#                         org = False
#                     groups = len(shared_with['groups'])

#                 if item_desc.type == 'Feature Service':
#                     # add to list of feature services
#                     if 'View Service' in item_desc.typeKeywords:
#                         is_view = True
#                         source_item_id = item_desc.related_items('Service2Service', 'reverse')[0].id
#                         source_item_name = item_desc.related_items('Service2Service', 'reverse')[0].title
#                     else:
#                         is_view = False
#                         source_item_id = None
#                         source_item_name = None
#                     dict_lists['FEATURE_SERVICES'].append(
#                         [item_desc.type, name, folder_desc, shared, everyone, org, groups, owner_desc, created, modified,
#                          itemid, is_view, source_item_id, source_item_name, size, content_status])
#                 elif item_desc.type == 'Web Map':
#                     # add to list of maps
#                     dict_lists['WEB_MAPS'].append(
#                         [item_desc.type, name, folder_desc, shared, everyone, org, groups, owner_desc, created, modified,
#                          itemid, size, content_status])
#                     webmap = arcgis.mapping.WebMap(item_desc)
#                     for layer in webmap.layers:
#                         layername = layer.title
#                         try:
#                             layer_item_id = layer.itemId
#                         except:
#                             layer_item_id = 'N/A'
#                         try:
#                             layerfilter = layer['layerDefinition']['definitionExpression']
#                         except KeyError:
#                             layerfilter = None
#                         try:
#                             layerurl = layer.url
#                         except:
#                             layerurl = None
#                         # add to Map/Layer Join Table
#                         dict_lists['MAP_FS_REL'].append([name, itemid, layername, layerfilter, layer_item_id, layerurl])
#                 elif item_desc.type == 'Web Mapping Application':
#                     appdata = item_desc.get_data()
#                     try:
#                         apptoMapID = appdata['map']['itemId']
#                     except:
#                         apptoMapID = None
#                     del (appdata)
#                     dict_lists['WEB_APPS'].append(
#                         [item_desc.type, name, folder_desc, shared, everyone, org, groups, owner_desc, created, modified,
#                          itemid, apptoMapID, size, content_status])
#                 else:
#                     dict_lists['OTHER_ITEMS'].append(
#                         [item_desc.type, name, folder_desc, shared, everyone, org, groups, owner_desc, created, modified,
#                          itemid, size, content_status])
#             except Exception as e:
#                 print_message(item_desc)
#                 errord.append(item_desc)
#                 print_message('something went wrong {}'.format(e))
#         queue.task_done()
#     return True




# sql_views = [
#     """CREATE VIEW APPS_TO_MAPS AS
#     SELECT *
#       FROM WEB_APPS WA
#            LEFT JOIN
#            WEB_MAPS WM ON WA.WEBMAP_ID = WM.ITEM_ID;"""
#     ,
#     """CREATE VIEW BROKEN_LAYERS AS
#     SELECT REL.WEBMAP_TITLE,
#            REL.LAYER_NAME,
#            FS.ITEM_NAME AS FS_NAME,
#            WM.SHARED AS WM_SHARED,
#            FS.SHARED AS FS_SHARED
#       FROM MAP_FS_REL REL
#            LEFT JOIN
#            WEB_MAPS WM ON REL.WEBMAP_ID = WM.ITEM_ID
#            LEFT JOIN
#            FEATURE_SERVICES FS ON REL.LAYER_ITEM_ID = FS.ITEM_ID
#      WHERE WM.SHARED <> FS.SHARED AND 
#            FS.SHARED = 'Private';"""
#     ,
#     """CREATE VIEW ALL_ITEMS AS
#     SELECT ITEM_TYPE,
#            ITEM_NAME,
#            FOLDER,
#            SHARED,
#            EVERYONE,
#            ORG,
#            GROUPS,
#            OWNER,
#            CREATEDATE,
#            MODATE,
#            ITEM_ID,
#            SIZE,
#            CONTENT_STATUS
#       FROM FEATURE_SERVICES
#     UNION ALL
#     SELECT ITEM_TYPE,
#            ITEM_NAME,
#            FOLDER,
#            SHARED,
#            EVERYONE,
#            ORG,
#            GROUPS,
#            OWNER,
#            CREATEDATE,
#            MODATE,
#            ITEM_ID,
#            SIZE,
#            CONTENT_STATUS
#       FROM WEB_MAPS
#     UNION ALL
#     SELECT ITEM_TYPE,
#            ITEM_NAME,
#            FOLDER,
#            SHARED,
#            EVERYONE,
#            ORG,
#            GROUPS,
#            OWNER,
#            CREATEDATE,
#            MODATE,
#            ITEM_ID,
#            SIZE,
#            CONTENT_STATUS
#       FROM WEB_APPS
#     UNION ALL
#     SELECT ITEM_TYPE,
#            ITEM_NAME,
#            FOLDER,
#            SHARED,
#            EVERYONE,
#            ORG,
#            GROUPS,
#            OWNER,
#            CREATEDATE,
#            MODATE,
#            ITEM_ID,
#            SIZE,
#            CONTENT_STATUS
#       FROM OTHER_ITEMS;
# """
#     ,
#     """CREATE VIEW SHARED_EVERYONE AS
#     SELECT *
#       FROM ALL_ITEMS
#      WHERE EVERYONE = 1
#      ORDER BY OWNER;"""
#     ,
#     """CREATE VIEW GROUPS_ZEROMEMBERS AS
#     SELECT *
#       FROM GROUPS
#      WHERE MEMBERCOUNT = 0;"""
#     ,
#     """CREATE VIEW USERS_NEVERLOGIN AS
#     SELECT *
#       FROM USERS
#      WHERE LAST_LOGIN = -1;"""
#     ,
#     """CREATE VIEW USERS_VIEW AS
#     SELECT USERNAME,
#            FIRSTNAME,
#            LASTNAME,
#            LEVEL,
#            ROLE,
#            CREATED,
#            LAST_LOGIN,
#            ROUND(JULIANDAY({}, 'unixepoch','localtime') - JULIANDAY(LAST_LOGIN),-2) AS DAYS_STAGNANT,
#            DESCRIPTION
#       FROM USERS;""".format(start_time)
#     ,
#     """CREATE VIEW OTO_FS_MAP AS
#     SELECT LAYER_ITEM_ID,
#            WEBMAP_ID
#       FROM MAP_FS_REL
#      GROUP BY LAYER_ITEM_ID,
#               WEBMAP_ID;"""
#     ,
#     """CREATE VIEW SHARING_GROUPS AS
#     SELECT
#       ITEM_ID,
#     COUNT(*) AS GROUP_COUNT,
#     GROUP_CONCAT(GROUP_NAME) AS GROUPS
#      FROM SHARING
#      GROUP BY ITEM_ID""",
#     """CREATE VIEW STORAGE_ESTIMATE AS 
#             SELECT
#             CATEGORY,
#             ITEMS,
#             PRINTF("%.2f", MB) AS MB,
#             FS_COUNT,
#             PRINTF("%.2f", FS_MB) AS FS_MB,
#             PRINTF("%.2f", FS_CPM) AS FS_CPM,
#             OTHER_COUNT,
#             PRINTF("%.2f", OTHER_MB) AS OTHER_MB,
#             PRINTF("%.2f", OTHER_CPM) AS OTHER_CPM,
#             PRINTF("%.2f", TOTAL_CPM) AS TOTAL_CPM
        
#         FROM
        
#         (SELECT
#             REPLACE(CATEGORIES, '/Categories/','') AS CATEGORY,
#             COUNT(*) AS ITEMS,
#             SUM(SIZE)/1024/1024 AS MB,
#             SUM(CASE WHEN ITEM_TYPE = 'Feature Service' THEN 1 ELSE 0 END) AS FS_COUNT,
#             SUM(CASE WHEN ITEM_TYPE = 'Feature Service' THEN SIZE/1024.0/1024.0 ELSE 0 END) AS FS_MB,
#             SUM(CASE WHEN ITEM_TYPE = 'Feature Service' THEN SIZE/1024.0/1024.0*.25 ELSE 0 END) AS FS_CPM,
#             SUM(CASE WHEN ITEM_TYPE <> 'Feature Service' THEN 1 ELSE 0 END) AS OTHER_COUNT,
#             SUM(CASE WHEN ITEM_TYPE <> 'Feature Service' THEN SIZE/1024.0/1024.0 ELSE 0 END) AS OTHER_MB,
#             SUM(CASE WHEN ITEM_TYPE <> 'Feature Service' THEN SIZE/1024.0/1024.0/1024.0*1.2 ELSE 0 END) AS OTHER_CPM,
#                 SUM(CASE WHEN ITEM_TYPE <> 'Feature Service' THEN SIZE/1024.0/1024.0/1024.0*1.2 ELSE 0 END) +
#                 SUM(CASE WHEN ITEM_TYPE = 'Feature Service' THEN SIZE/1024.0/1024.0*.25 ELSE 0 END)
#             AS TOTAL_CPM
            
#         FROM
#             ALL_ITEMS AI
#         LEFT JOIN
#             (SELECT
#             ITEM_ID,
#             GROUP_CONCAT(CATEGORY) AS CATEGORIES,
#             COUNT(*) AS TOTALCATS
#             FROM CATEGORIES
#             GROUP BY ITEM_ID) CATS
#         ON AI.ITEM_ID = CATS.ITEM_ID
#         GROUP BY REPLACE(CATEGORIES, '/Categories/','') 
#         )""",
#     """CREATE VIEW ITEM_CTGS as
#             SELECT
#             AI.ITEM_NAME,
#             AI.ITEM_TYPE,
#             AI.OWNER,
#             AI.FOLDER,
#             AI.SHARED,
#             AI.SIZE/1024/1024 AS MB,
#             TT.TAGS,
#             SG.GROUPS,
#             CATS.CATEGORIES,
#             AI.ITEM_ID        
#         FROM
#             ALL_ITEMS AI
#         LEFT JOIN
#             (SELECT
#             ITEM_ID,
#             GROUP_CONCAT(REPLACE(CATEGORY,'/Categories/','')) AS CATEGORIES,
#             COUNT(*) AS TOTALCATS
#             FROM CATEGORIES
#             GROUP BY ITEM_ID) CATS
#         ON 
#             AI.ITEM_ID = CATS.ITEM_ID
#         LEFT JOIN
#             (SELECT
#                 ITEM_ID,
#                 GROUP_CONCAT(TAG) AS TAGS
#             FROM TAGS
#             GROUP BY ITEM_ID) TT
#         ON 
#             AI.ITEM_ID = TT.ITEM_ID
#         LEFT JOIN
#             SHARING_GROUPS SG
#         ON 
#             AI.ITEM_ID = SG.ITEM_ID"""
#     ]

# conn = sqlite3.connect(db)
# for key in data_inventory:
#     dlist_to_sqlite(data_inventory[key], conn, key)

# cursor = sqlite3.Cursor(conn)
# for statement in sql_views:
#     cursor.execute(statement)
# conn.close()



# #===========================================================================
# # Next one
# #===========================================================================


# # arcgis org administrator info
# org = input('org: ')
# username = input('username: ')
# password = input('password: ')
# justme = input('Just This User? Y/N: ')
# org_url = 'https://{}.maps.arcgis.com'.format(org)
# folder = input('Destination DB Folder: ')
# num_threads = int(input('Thread Count: '))



# import arcgis
# import arcpy
# import time
# import pandas
# import sqlite3
# from time import strftime
# import os
# import threading
# from queue import Queue


# def print_message(message):
#     print(message)
#     arcpy.AddMessage(message)

# def online_to_pst_time(time_value):
#     pst_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime((time_value / 1000.0) - 25200))
#     return pst_time


# def dlist_to_sqlite(dlist, connection, table_name, **kwargs):
#     df = pandas.DataFrame(dlist[1:], columns=dlist[0])
#     df.index.name = 'OID'
#     df.to_sql(table_name, connection, if_exists='replace', **kwargs)
#     connection.commit()














# # output sqlite database
# todate = strftime("%Y%m%d")
# start_time = time.time()
# if not os.path.exists(os.path.join(folder, org)):
#     os.mkdir(os.path.join(folder, org))
# db = os.path.join(folder, org, "{}_{}.sqlite".format(org, todate))

# GIS = arcgis.GIS(org_url, username, password)
# data_inventory = {
#     'FEATURE_SERVICES': [
#         ['ITEM_TYPE', 'ITEM_NAME', 'FOLDER', 'SHARED', 'EVERYONE', 'ORG', 'GROUPS', 'OWNER', 'CREATEDATE', 'MODATE',
#          'ITEM_ID', 'ISVIEW', 'SOURCE_ITEM_ID', 'SOURCE_ITEM_NAME', 'SIZE', 'CONTENT_STATUS']],
#     'GROUP_MEMBERSHIP': [
#         ['GROUP_NAME', 'MEMBER', 'MEMBERTYPE']],
#     'GROUPS': [
#         ['GROUP_NAME', 'OWNER', 'ADMINCOUNT', 'MEMBERCOUNT', 'ID', 'CREATEDATE', 'ITEMS']],
#     'MAP_FS_REL': [
#         ['WEBMAP_TITLE', 'WEBMAP_ID', 'LAYER_NAME', 'EDITABLE', 'LAYER_FILTER', 'LAYER_ITEM_ID', 'LAYER_URL']],
#     'WEB_APPS': [
#         ['ITEM_TYPE', 'ITEM_NAME', 'FOLDER', 'SHARED', 'EVERYONE', 'ORG', 'GROUPS', 'OWNER', 'CREATEDATE', 'MODATE',
#          'ITEM_ID', 'WEBMAP_ID', 'SIZE', 'CONTENT_STATUS']],
#     'WEB_MAPS': [
#         ['ITEM_TYPE', 'ITEM_NAME', 'FOLDER', 'SHARED', 'EVERYONE', 'ORG', 'GROUPS', 'OWNER', 'CREATEDATE', 'MODATE',
#          'ITEM_ID', 'SIZE', 'CONTENT_STATUS']],
#     'OTHER_ITEMS': [
#         ['ITEM_TYPE', 'ITEM_NAME', 'FOLDER', 'SHARED', 'EVERYONE', 'ORG', 'GROUPS', 'OWNER', 'CREATEDATE', 'MODATE',
#          'ITEM_ID', 'SIZE', 'CONTENT_STATUS']],
#     'SHARING': [
#         ['ITEM_ID', 'GROUP_NAME']],
#     'USERS': [
#         ['USERNAME', 'FIRSTNAME', 'LASTNAME', 'LEVEL', 'ROLE', 'CREATED', 'LAST_LOGIN', 'DESCRIPTION']],
#     'CATEGORIES': [
#         ['ITEM_ID', 'CATEGORY']],
#     'TAGS': [
#         ['ITEM_ID', 'TAG']]
# }

# folder_dict = {None: None}




# # for group in GIS.users.me.groups:
# #     for item in group.content():
# #         data_inventory['SHARING'].append([item.id, group.title])

# print_message('Starting User Scan...')
# users = GIS.users.search()
# for user in users:
#     username = user.username
#     created = online_to_pst_time(user.created)
#     firstname = user.firstName
#     lastname = user.lastName
#     level = user.level
#     roleID = user.roleId
#     description = user.description
#     last_login = online_to_pst_time(user.lastLogin)
#     data_inventory['USERS'].append([username, firstname, lastname, level, roleID, created, last_login, description])
#     for folder in user.folders:
#         folder_dict[folder['id']] = folder['title']

# print_message('Starting Item Scan...')
# errord = []
# try:
#     if justme == 'Y':
#         dupe_itemList = GIS.content.advanced_search('owner:{}'.format(username), max_items=9999)['results']
#     else:
#         dupe_itemList = GIS.content.advanced_search('accountid: {}'.format(GIS.properties.get('id')), max_items = 9999)['results']
# except:
#     if justme == 'Y':
#         dupe_itemList = GIS.content.search('owner:{}'.format(username), max_items=9999)
#     else:
#         dupe_itemList = GIS.content.search('*'.format(GIS.properties.get('id')), max_items = 9999)

# # REMOVE DUPES FROM ITEM LIST
# uniques = []
# dupes = []
# itemList = []
# for item in dupe_itemList:
#     if item.id in uniques:
#         dupes.append(item.id)
#     else:
#         itemList.append(item)
#         uniques.append(item.id)

# print_message('\tTotal Items: ' + str(len(itemList)))
# # start_time = time.time()




# q = Queue(maxsize=0)

# for i in range(len(itemList)):
#     q.put((i, itemList[i]))

# for i in range(num_threads):
#     print_message('\tStarting thread {}'.format(i))
#     worker = threading.Thread(target=item_descrip, args=(q,data_inventory))
#     worker.setDaemon(True)
#     worker.start()

# q.join()
# # end_time = time.time()
# sql_views = [
#     """CREATE VIEW APPS_TO_MAPS AS
#     SELECT *
#       FROM WEB_APPS WA
#            LEFT JOIN
#            WEB_MAPS WM ON WA.WEBMAP_ID = WM.ITEM_ID;"""
#     ,
#     """CREATE VIEW BROKEN_LAYERS AS
#     SELECT REL.WEBMAP_TITLE,
#            REL.LAYER_NAME,
#            FS.ITEM_NAME AS FS_NAME,
#            WM.SHARED AS WM_SHARED,
#            FS.SHARED AS FS_SHARED
#       FROM MAP_FS_REL REL
#            LEFT JOIN
#            WEB_MAPS WM ON REL.WEBMAP_ID = WM.ITEM_ID
#            LEFT JOIN
#            FEATURE_SERVICES FS ON REL.LAYER_ITEM_ID = FS.ITEM_ID
#      WHERE WM.SHARED <> FS.SHARED AND 
#            FS.SHARED = 'Private';"""
#     ,
#     """CREATE VIEW ALL_ITEMS AS
#     SELECT ITEM_TYPE,
#            ITEM_NAME,
#            FOLDER,
#            SHARED,
#            EVERYONE,
#            ORG,
#            GROUPS,
#            OWNER,
#            CREATEDATE,
#            MODATE,
#            ITEM_ID,
#            SIZE,
#            CONTENT_STATUS
#       FROM FEATURE_SERVICES
#     UNION ALL
#     SELECT ITEM_TYPE,
#            ITEM_NAME,
#            FOLDER,
#            SHARED,
#            EVERYONE,
#            ORG,
#            GROUPS,
#            OWNER,
#            CREATEDATE,
#            MODATE,
#            ITEM_ID,
#            SIZE,
#            CONTENT_STATUS
#       FROM WEB_MAPS
#     UNION ALL
#     SELECT ITEM_TYPE,
#            ITEM_NAME,
#            FOLDER,
#            SHARED,
#            EVERYONE,
#            ORG,
#            GROUPS,
#            OWNER,
#            CREATEDATE,
#            MODATE,
#            ITEM_ID,
#            SIZE,
#            CONTENT_STATUS
#       FROM WEB_APPS
#     UNION ALL
#     SELECT ITEM_TYPE,
#            ITEM_NAME,
#            FOLDER,
#            SHARED,
#            EVERYONE,
#            ORG,
#            GROUPS,
#            OWNER,
#            CREATEDATE,
#            MODATE,
#            ITEM_ID,
#            SIZE,
#            CONTENT_STATUS
#       FROM OTHER_ITEMS;
# """
#     ,
#     """CREATE VIEW SHARED_EVERYONE AS
#     SELECT *
#       FROM ALL_ITEMS
#      WHERE EVERYONE = 1
#      ORDER BY OWNER;"""
#     ,
#     """CREATE VIEW GROUPS_ZEROMEMBERS AS
#     SELECT *
#       FROM GROUPS
#      WHERE MEMBERCOUNT = 0;"""
#     ,
#     """CREATE VIEW USERS_NEVERLOGIN AS
#     SELECT *
#       FROM USERS
#      WHERE LAST_LOGIN = -1;"""
#     ,
#     """CREATE VIEW USERS_VIEW AS
#     SELECT USERNAME,
#            FIRSTNAME,
#            LASTNAME,
#            LEVEL,
#            ROLE,
#            CREATED,
#            LAST_LOGIN,
#            ROUND(JULIANDAY({}, 'unixepoch','localtime') - JULIANDAY(LAST_LOGIN),-2) AS DAYS_STAGNANT,
#            DESCRIPTION
#       FROM USERS;""".format(start_time)
#     ,
#     """CREATE VIEW OTO_FS_MAP AS
#     SELECT LAYER_ITEM_ID,
#            WEBMAP_ID
#       FROM MAP_FS_REL
#      GROUP BY LAYER_ITEM_ID,
#               WEBMAP_ID;"""
#     ,
#     """CREATE VIEW SHARING_GROUPS AS
#     SELECT
#       ITEM_ID,
#     COUNT(*) AS GROUP_COUNT,
#     GROUP_CONCAT(GROUP_NAME) AS GROUPS
#      FROM SHARING
#      GROUP BY ITEM_ID"""
#     ]

# conn = sqlite3.connect(db)
# for key in data_inventory:
#     dlist_to_sqlite(data_inventory[key], conn, key)

# cursor = sqlite3.Cursor(conn)
# for statement in sql_views:
#     cursor.execute(statement)
# conn.close()





