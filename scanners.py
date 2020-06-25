

import arcgis
# import arcpy
import time
import pandas
import sqlite3
from threading import Thread
from queue import Queue


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
        # start = time.time()
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
                try:
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
                except:
                    everyone = False
                    org = False
                    groups = 99

            if item_desc.type == 'Feature Service':
                # add to list of feature services
                if 'View Service' in item_desc.typeKeywords:
                    is_view = True
                    try:
                        source_item_id = item_desc.related_items('Service2Service', 'reverse')[0].id
                        source_item_name = item_desc.related_items('Service2Service', 'reverse')[0].title
                    except:
                        source_item_id = None
                        source_item_name = None
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
                
                try:
                    appdata = item_desc.get_data()
                    apptoMapID = appdata['map']['itemId']
                    del (appdata)
                except:
                    apptoMapID = None

                dict_lists['WEB_APPS'].append(
                    [item_desc.type, name, folder_desc, shared, everyone, org, groups, owner_desc, created, modified,
                     itemid, apptoMapID, size, content_status])
            else:
                dict_lists['OTHER_ITEMS'].append(
                    [item_desc.type, name, folder_desc, shared, everyone, org, groups, owner_desc, created, modified,
                     itemid, size, content_status])
        except Exception as e:
            print(item_desc)
            print('something went wrong')
            print(e)
        queue.task_done()
        # duration = time.time() - start 
        # print(name, duration)
    return True


def item_scan(gis_object, dict_lists, folder_dict, num_threads=15, depth='org'):
    '''
    

    Parameters
    ----------
    gis_object : arcgis.GIS
        arcgis GIS object containing your login information.
    dict_lists : dictionary
        prepared by another function in this. stores information from your scan.
    folder_dict : TYPE
        DESCRIPTION.
    num_threads : str
        Number of threads to use in scan.  ArcGIS Online performs fine with 15.
        For ArcGIS Portal, you may want to drop the number
    depth : str, optional
        Level to which you want to scan. 
        'user' : limits the scan to only the user.
        'org' : only items in the user's organization
        'extended' : scans all available items to user. Including shared items
            from other organizations. 
        The default is 'org'.

    Returns
    -------
    None.

    '''
    try:
        if depth == 'user':
            item_list = gis_object.content.advanced_search('owner:{}'.format(gis_object.users.me.username), max_items=9999)['results']
        else:
            item_list = gis_object.content.advanced_search('accountid: {}'.format(gis_object.properties.get('id')), 
                                                               max_items = 9999)['results']
    except:
        if depth == 'user':
            item_list = gis_object.content.search('owner:{}'.format(gis_object.users.me.username), max_items=9999)
        else:
            item_list = gis_object.content.search('*', max_items = 9999)
    
    if depth == 'extended':
        itemids = [item.id for item in item_list]
        for itemid in dict_lists['temp_shared_items']:
            if itemid not in itemids:
                item_list.append(dict_lists['temp_shared_items'][itemid])
        
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
    
    dict_lists.pop('temp_shared_items',None)
    
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
            ['ITEM_ID', 'TAG']],
        'temp_shared_items' : {}
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
            dict_lists['temp_shared_items'][item.id] = item
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
        
    if num_threads > len(group):
        num_threads = len(group)    
    
    for i in range(num_threads):
        worker = Thread(target=group_grab, args=(q, dict_lists))
        worker.setDaemon(True)
        worker.start()

    q.join()   
    
    
    
def user_grab(queue, dict_lists, folder_dict, role_dict):
    while not queue.empty():
        work = queue.get()
        user = work[1]
        username = user.username
        created = online_to_pst_time(user.created)
        try:
            firstname = user.firstName
            lastname = user.lastName
        except AttributeError:
            firstname = ''
            lastname = ''
        level = user.level
        try:
            roleID = role_dict[user.roleId]
        except KeyError:
            roleID = user.roleId
        description = user.description
        last_login = online_to_pst_time(user.lastLogin)
        dict_lists['USERS'].append([username, firstname, lastname, level, roleID, created, last_login, description])
        try:
            for folder in user.folders:
                folder_dict[folder['id']] = folder['title']
        except:
            pass
        queue.task_done()
    return True


def user_scan(gis_object, dict_lists, num_threads):
    users = gis_object.users.search(max_users=9999)
    folder_dict = {None: None}
    
    role_dict = {role.role_id: role.name for role in arcgis.gis.RoleManager(gis_object).all()}
    
    q = Queue(maxsize=0)

    for i, user in enumerate(users):
        q.put((i, user))
        
    if num_threads > len(users):
        num_threads = len(users)
    
    for i in range(num_threads):
        worker = Thread(target=user_grab, args=(q, dict_lists, folder_dict, role_dict))
        worker.setDaemon(True)
        worker.start()

    q.join()   
    return folder_dict
    

