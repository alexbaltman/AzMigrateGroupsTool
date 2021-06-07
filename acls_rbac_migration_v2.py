# Proof of Concept Code for a python based Storage Account migration w/ ACLs
# SDK docs: https://azuresdkdocs.blob.core.windows.net/$web/python/azure-storage-file-datalake/12.0.0b7/azure.storage.filedatalake.html
# SDK github samples: https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/storage/azure-storage-file-datalake/samples/datalake_samples_upload_download.py

import os

from azure.core.exceptions import AzureError
from azure.storage.filedatalake import DataLakeServiceClient

storage_account_name_from = os.getenv('STORAGE_ACCOUNT_NAME_FROM', "")
storage_account_key_from = os.getenv('STORAGE_ACCOUNT_KEY_FROM', "")
storage_account_name_to = os.getenv('STORAGE_ACCOUNT_NAME_TO', "")
storage_account_key_to = os.getenv('STORAGE_ACCOUNT_KEY_TO', "")

if __name__ == '__main__':
    # Pre-req: Create storage account via ARM template in portal from Storage account "From" for the "To" storage account.

    #Create a client to interact with the storage accounts
    service_client_from = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https",
            storage_account_name_from
        ), credential=storage_account_key_from)

    service_client_to = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https",
            storage_account_name_to
        ), credential=storage_account_key_to)


# Iterate through all containers in the FROM storage account
    for myfromfilesystem in service_client_from.list_file_systems():
        myfromFSclient = service_client_from.get_file_system_client(myfromfilesystem)

        mytoFSclient = service_client_to.create_file_system(myfromfilesystem)
        
        # Iterate through all paths in the "From" filesystem and create the "To" path item (directory or file) with same acls
        for myfrompath in myfromFSclient.get_paths():
            if myfrompath.is_directory:

                # Get "From" directory properties
                fromdirclient = myfromFSclient.get_directory_client(myfrompath.name)
                #fromdirproperties = fromdirclient.get_directory_properties()
                fromdirproperties = fromdirclient._get_path_properties()
                fromdiracls = fromdirclient.get_access_control()
                
                # Create dir in "To" directory with same properties as "From"
                todirclient = mytoFSclient.get_directory_client(fromdirproperties.name)
                # Can add permissions= in create_directory method if you want to combine these next two steps.
                todirclient.create_directory(content_settings=fromdirproperties.content_settings)
                todirclient.set_access_control(acl=fromdiracls['acl'])
                
                print("Created: " + myfrompath.name)
            else:
                # Create client to interact with "From" file
                fromfileclient = myfromFSclient.get_file_client(myfrompath.name)
                
                # Get "From" file properties
                fromfileproperties = fromfileclient.get_file_properties()
                fromfileacls = fromfileclient.get_access_control()
                
                # Download "From" file locally
                localfile = os.path.dirname(__file__) + '\\' + os.path.basename(fromfileclient.path_name)
                print("Downloading data to '{}'.".format(localfile))
                with open(localfile, 'wb') as stream:
                    fromfile = fromfileclient.download_file()
                    fromfile.readinto(stream)

                # Create client to interact with "To" file using same path as "From". It does not have to exist yet.
                tofileclient = mytoFSclient.get_file_client(myfrompath.name)
                
                # create "To" file with same properties as "From" file. 
                tofileclient.create_file(content_settings=fromfileproperties.content_settings)
                tofileclient.set_access_control(acl=fromfileacls['acl'])
                # upload file content and write (flush) it
                # Max Byte upload before failure: 50552832. Have not tested larger file. Can likely chunk it further. 
                print("Uploading data from '{}'.".format(localfile))
                with open(localfile, "rb") as myfromfilecontent:
                    totalsize = 0
                    chunksize = 1024

                    chunk = myfromfilecontent.read(chunksize)
                    while chunk:
                        tofileclient.append_data(data=chunk, offset=totalsize, length=len(chunk))
                        totalsize += len(chunk)
                        chunk = myfromfilecontent.read(chunksize)
                    tofileclient.flush_data(totalsize)
                print("Copied:" + myfrompath.name)