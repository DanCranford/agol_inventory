
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
         ,
    """
        CREATE VIEW STORAGE_ESTIMATE AS
        
        SELECT
            AI.ITEM_ID,
            AI.ITEM_NAME,
            AI.ITEM_TYPE,
            AI.FOLDER,
            AI.OWNER,
            REPLACE(CATEGORIES, '/Categories/','') AS CATEGORY,
            SIZE/1024/1024 AS MB,
            CASE WHEN ITEM_TYPE = 'Feature Service' THEN SIZE/1024.0/1024.0 ELSE 0 END AS FS_MB,
            CASE WHEN ITEM_TYPE = 'Feature Service' THEN SIZE/1024.0/1024.0*.25 ELSE 0 END AS FS_CPM,
            CASE WHEN ITEM_TYPE <> 'Feature Service' THEN SIZE/1024.0/1024.0 ELSE 0 END AS OTHER_MB,
            CASE WHEN ITEM_TYPE <> 'Feature Service' THEN SIZE/1024.0/1024.0/1024.0*1.2 ELSE 0 END AS OTHER_CPM,
                CASE WHEN ITEM_TYPE <> 'Feature Service' THEN SIZE/1024.0/1024.0/1024.0*1.2 ELSE 0 END +
                CASE WHEN ITEM_TYPE = 'Feature Service' THEN SIZE/1024.0/1024.0*.25 ELSE 0 END
            AS TOTAL_CPM
            
        FROM
            ALL_ITEMS AI
        LEFT JOIN
            (SELECT
            ITEM_ID,
            GROUP_CONCAT(CATEGORY) AS CATEGORIES,
            COUNT(*) AS TOTALCATS
            FROM CATEGORIES
            GROUP BY ITEM_ID) CATS
        ON AI.ITEM_ID = CATS.ITEM_ID
        LEFT JOIN
            USERS USERS
        ON AI.OWNER = USERS.USERNAME
        WHERE USERS.USERNAME IS NOT NULL

        """,
    """CREATE VIEW ITEM_CTGS as
            SELECT
            AI.ITEM_NAME,
            AI.ITEM_TYPE,
            AI.OWNER,
            AI.FOLDER,
            AI.SHARED,
            AI.SIZE/1024/1024 AS MB,
            TT.TAGS,
            SG.GROUPS,
            CATS.CATEGORIES,
            AI.ITEM_ID        
        FROM
            ALL_ITEMS AI
        LEFT JOIN
            (SELECT
            ITEM_ID,
            GROUP_CONCAT(REPLACE(CATEGORY,'/Categories/','')) AS CATEGORIES,
            COUNT(*) AS TOTALCATS
            FROM CATEGORIES
            GROUP BY ITEM_ID) CATS
        ON 
            AI.ITEM_ID = CATS.ITEM_ID
        LEFT JOIN
            (SELECT
                ITEM_ID,
                GROUP_CONCAT(TAG) AS TAGS
            FROM TAGS
            GROUP BY ITEM_ID) TT
        ON 
            AI.ITEM_ID = TT.ITEM_ID
        LEFT JOIN
            SHARING_GROUPS SG
        ON 
            AI.ITEM_ID = SG.ITEM_ID"""
        ]

    conn = sqlite3.connect(sqlite_path)
    for key in dict_lists:
        dlist_to_sqlite(dict_lists[key], conn, key)

    cursor = sqlite3.Cursor(conn)
    for statement in sql_views:
        cursor.execute(statement)
    conn.close()


def output_to_excel(dict_lists, output_excel):
    with pandas.ExcelWriter(output_excel) as xl_writer:
        for key in dict_lists:
            df = pandas.DataFrame(dict_lists[key][1:], columns = dict_lists[key][0])
            df.to_excel(xl_writer, sheet_name = key, index=False)
    

