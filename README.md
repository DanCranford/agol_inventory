# AGOL Inventory

## User Inventory

The first thing that the tool does by default is to gather a list of all the users in your organization.  From this, you'll get a table with the following information (where available) about each user:
- Username
- First Name
- Last Name
- User Description / Bio
- User Creation Date
- Last Login Date

This has proven to be pretty helpful in managing accounts.  Specifically, it's pretty helpful for identifying users that haven't logged in recently.

## Group Inventory

This portion of the tool retreives information about all the groups in your organization or groups that you have access to.  This might include external groups that your user has joined.  This can be helpful if you have shared items or maps that are built off shared data sources.  This portion of the tool will give you three different tables containing the following:
- Group Summary - for each group
  - Group Name
  - Group Owner
  - Count of group administrators
  - Count of group members
  - Group ID (unique identifier)
  - Group Creation Date
  - Count of items shared with group
- Group Membership - for each member in each group
  - Group Name
  - Member Username
  - Member Type (admin/owner/member)
- Sharing - for each item shared with each group
  - Item ID 
  - Group Name
 

## Item Inventory

This is where this tool really shines.  If you're in a situation where you have to manage a bunch of inter-related items in ArcGIS Online, this is going to be super useful.  This can help you document the relationships between data, web maps, and web apps on a macro level.

The way the tool does this is that it outputs the following:

- All Items
  - Item Name
  - Item ID (unique identifier)
  - Item Type (Web Map, Form, Web Application, Feature Service, etc)
  - Item sharing summary
  - Item Owner
  - Item created date
  - Item size
  - Item content status (Authoritative or Depricated)
- Feature Services - additional information
  - View / Source Info
- Web Maps - additional information
  - Layers contained in web map
- Web Mapping Applications
  - Web Map related item
