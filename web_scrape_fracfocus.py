#Import desired packages 
import requests
import zipfile
import io
import pandas as pd

def pull_zip_file_from_url(url):
    """
    This function pulls a zip file from a URL and generates a ZipFile object
    Arguments:
        url: String. Name of the URL that we want to pull from
    Outputs:
        zip_file: ZipFile object, generated from the URL
    """
    request = requests.get(url)
    zip_file = zipfile.ZipFile(io.BytesIO(request.content))
    return zip_file

def append_filename_to_desired_list(list_to_append_to, key_word, file_name):
    """
    This function detects if the filename in the .zip folder is a csv, and if it contains a specific key_word string. If it 
    does, it appends to the desired master list of filenames
    Arguments:
        list_to_append_to: List. Master list containing filenames that end with .csv extension, and contain the key_word value
        key_word: String. Identifier that the filename must contain in order to be append to the list_to_append_to list.
        file_name: String. Name of the file that we're checking, contained in the .zip folder
    Outputs:
        list_to_append_to: List. Master list, with new filename appended if it is a .csv extension and contains the key_word string.
    """
    if ((file_name.endswith('.csv')) & (key_word in file_name)):
        list_to_append_to.append(file_name)
    return list_to_append_to

def append_dataframes_into_master(list_of_filenames, zip_file):
    """
    This function generates a master dataframe, pulled from all of the dataframes with filenames in the list_of_filenames list
    Arguments: 
        list_of_filenames: List. List of filenames that we want to pull the data from. 
        zip_file: ZipFile object. Zipfile that we're pulling files from
    Outputs:
        df_master: Pandas dataframe. Master dataframe containing all the data associated with the filenames from list_of_filenames 
        list.
    """
    list_of_dfs=[pd.read_csv(zip_file.open(x), low_memory=False) for x in list_of_filenames]
    df_master = pd.concat([r for r in list_of_dfs], ignore_index=True)
    return df_master

def main():
    #Name the FracFocus URL
    fracfocus_url='http://fracfocusdata.org/digitaldownload/fracfocuscsv.zip'
    #Create a ZipFile object from the pulled URL .zip file
    zip_file = pull_zip_file_from_url(fracfocus_url)
    #Get the list of file names in the zip file
    list_of_file_names = zip_file.namelist()
    #Create lists for storing the registryupload files, and the FracFocus registry files
    list_fracfocus_registry_files=[]
    list_registry_upload_files=[]
    # Iterate over the file names
    for file_name in list_of_file_names:
        # Check filename endswith csv, and is one of the FracFocusRegistry files
        list_fracfocus_registry_files=append_filename_to_desired_list(list_fracfocus_registry_files, 'FracFocusRegistry', file_name)
        #Detect if the filename ends with csv, and is one of the registryupload files
        list_registry_upload_files=append_filename_to_desired_list(list_registry_upload_files, 'registryupload', file_name)
    #Append the registry upload files into a master df
    registry_upload_master=append_dataframes_into_master(list_registry_upload_files, zip_file)
    #Append the FracFocus registry files into a master df
    fracfocus_registry_master=append_dataframes_into_master(list_fracfocus_registry_files, zip_file)
    #Write both files to their respective master csv's
    registry_upload_master.to_csv('registry_upload_master.csv')
    fracfocus_registry_master.to_csv('fracfocus_registry_master.csv')
    
if __name__== "__main__":
    main()


