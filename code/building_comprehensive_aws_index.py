#########################################
#
# building_comprehensive_aws_index.py
#----------------------------------
#
# This script fetches information to build onto the AWS index of files it has on hand
# but are not included in its current index.
#
#----------------------------------
#
# Notes: This script takes a LONG time, it is not efficient time-wise. Estimated at
# 	~1.0s for each file that needs to be retrieved, depending on your internet connection.
# This uses the irsx package to read files 2015 and later.
# It assumes you used the included file retrieval script to get file names.
# Initially run in a jupyter notebook, it has not been adapted competently into this
# 	script (Ex. print statements). However, the memory handling of jupyter makes this
#	inefficient memory-wise and so a script is preferred when reading many files.
# I am also a complete novice in XML, so the manual XML reading is inelegant, but works.
#
#----------------------------------
#
# By: Simon Yamawaki Shachter
# Date: May 6, 2021
#
#########################################

# Libraries
import pandas as pd
import re
from irsx.xmlrunner import XMLRunner

# Constants
# - File names are assumed to include the year as text at some point in the name
# - Current file is assumed to be the index file downloaded from AWS, but this is flexible.
# - New OID file is assumed to be the file list file from the associated retrieval script.
# - New index file is what you want to save it as
CUR_IND_FILE_PREF = "index_"
CUR_IND_FILE_SUFF = ".csv"
IND_FILE_OID_COL = "OBJECT_ID"
NEW_OID_FILE_PREF = "file_list_"
NEW_OID_FILE_SUFF = ".csv"
NEW_OID_FILE_COL = "file_name"
NEW_IND_FILE_PREF = "all_file_index_new_"
NEW_IND_FILE_SUFF = ".csv"
BEGIN_YR = 2009
END_YR = 2019


#########################################
# This script is split up by fetching 990 information by relying on irsx and doing
# so manually. There are a different set of functions depending.
#########################################

#########################################
# Manual index information fetch
#########################################

# Nearly all 990 XMLs not covered by IRSx can use the below xml mapping
EIN_MAP = {'gen': 'EIN\\>\d.{7,10}', 'spec': '\d+', 'start': 0}
MANU_INFO_MAP = {'EIN': EIN_MAP,
                 'NAME1': {'gen': '\\<Name\\>.*\\r\\n.*\\<BusinessNameLine1\\>.*\\<\\/BusinessNameLine1\\>',
                           'spec': '1\\>.*\\<',
                           'start': 2},
                 'NAME2': {'gen': '\\<Name\\>.*\\r\\n.*\\r\\n\\<BusinessNameLine2\\>.*\\<\\/BusinessNameLine2\\>',
                           'spec': '2\\>.*\\<',
                           'start': 2},
                 'RETURN_TYPE': {'gen': 'ReturnType\\>990.*\\<',
                                 'spec': '990.*\\<',
                                 'start': 0}}
# Version 2013v3.1 and v3.0 has a different mapping
MANU_INFO_MAP_ALT = {'EIN': EIN_MAP,
                     'NAME1': {'gen': '\\<BusinessName\\>.*\\r\\n.*\\<BusinessNameLine1\\>.*\\<\\/BusinessNameLine1\\>',
                               'spec': '1\\>.*\\<',
                               'start': 2},
                     'NAME2': {'gen': '\\<BusinessName\\>.*\\r\\n.*\\r\\n\\<BusinessNameLine2\\>.*\\<\\/BusinessNameLine2\\>',
                               'spec': '2\\>.*\\<',
                               'start': 2},
                     'RETURN_TYPE': {'gen': 'ReturnTypeCd\\>990.*\\<',
                                     'spec': '990.*\\<',
                                     'start': 0}}


# Most versions have identical mapping, but two have an alternative mapping
ALT_VERS = ['2013v3.1', '2013v3.0']
def manu_det_alt_vers( form_990 ):
    try:
        vers = re.search( '\d.*', re.search( 'returnVersion=".{8,10}"', form_990 )[0] )[0][:-1]
    except:
        vers = ''
    return ( vers in ALT_VERS )


# Fetch specific information manually
def manu_fetch_info( form_990, info_col ):
    
    # Determine which mapping to use
    info_map = MANU_INFO_MAP_ALT if manu_det_alt_vers( form_990 ) else MANU_INFO_MAP
    # Manage taxpayer name differently
    if info_col == "TAXPAYER_NAME":
        return manu_fetch_name( form_990 )
    else:
        try:
            # Finding the info takes three steps, finding where it is in the form, two levels of
            # extraction from that region
            gen_str = re.search( info_map[info_col]['gen'], form_990 )[0]
            spec_str = re.search( info_map[info_col]['spec'], gen_str )[0]
            end_loc = len( spec_str ) if info_col == 'EIN' else -1
            return spec_str[info_map[info_col]['start']:end_loc]
        except:
            return ''

# Get the two potential name lines from IRSx and combine them
def manu_fetch_name( form_990 ):
    name1 = manu_fetch_info( form_990, "NAME1" )
    name2 = manu_fetch_info( form_990, "NAME2" )
    full_name = re.sub( '&AMP;', '&', ( name1 + ' ' + name2 ).strip().upper() )
    return full_name

# Fetch file directly from AWS.
# Sometimes the request raises an error but still works well enough so there are two try statements
def manu_fetch_file( oid ):
    try:
        url = 'https://s3.amazonaws.com/irs-form-990/' + oid + '_public.xml'
        r = requests.get( url, allow_redirects=True )
    except:
        print( "Potential difficulty reading Object ID: " + oid )
    try:
        return r.text
    except:
        return None


#########################################
# IRSx index information fetch
#########################################


# Maps column names onto IRSx concordance file names
IRSX_INFO_MAP = { 'EIN': 'ein',
                  'NAME1': 'BsnssNm_BsnssNmLn1Txt',
                  'NAME2': 'BsnsssNmLn2Txt',
                  'RETURN_TYPE': 'RtrnHdr_RtrnCd' }


# Get a specific value from IRSx
def irsx_fetch_info( form_990, info_col ):
    # Because taxpayer name requires a little more processing, send it to a different function
    # Otherwise read the info from the form as determined by the dictionary
    if info_col == "TAXPAYER_NAME":
        return irsx_fetch_name( form_990 )
    else:
        try:
            return form_990[0]['schedule_parts']['returnheader990x_part_i'][IRSX_INFO_MAP[info_col]]
        except:
            return ''


# Get the two name lines from IRSx and combine them
def irsx_fetch_name( form_990 ):
    name1 = irsx_fetch_info( form_990, "NAME1" )
    name2 = irsx_fetch_info( form_990, "NAME2" )
    full_name = re.sub( '&AMP;', '&', ( name1 + ' ' + name2 ).strip().upper() )
    return full_name


# Fetch file from IRSx
def irsx_fetch_file( xml_runner, oid ):
    try:
        return xml_runner.run_sked( oid, 'ReturnHeader990x' ).get_result()
    except:
        print( "Difficulty reading file " + str( oid ) + " from IRSx.")
        return None


#########################################
# Wrappers
# Two wrappers, one that cycles through Object IDs and one that cycles through index column values.
#########################################

# Information to fetch from 990s for the index file
IND_COLS = ['EIN', 'TAXPAYER_NAME', 'RETURN_TYPE']

# Fetch a row of information for the index file
def fetch_ind_row( irsx, xml_runner, oid ):
    
    # Fetch 990 File
    if irsx:
        form_990 = irsx_fetch_file( xml_runner, oid )
    else:
        form_990 = manu_fetch_file( oid )
    
    # Fetch specific values from the file
    ind_info = pd.DataFrame( {IND_FILE_OID_COL: oid}, index=[oid] )
    for info_col in IND_COLS:
        if irsx:
            ind_info[info_col] = irsx_fetch_info( form_990, info_col )
        else:
            ind_info[info_col] = manu_fetch_info( form_990, info_col )
    
    return ind_info


def fetch_yr_ind( oid_srch_lst ):

    # Should we use IRSx or manual concordance? Setup IRSx if using it
    # Requires all object IDs in the file to be from the same year
    irsx_flag = True if int( oid_srch_lst[0][:4] ) >= 2015 else False
    xml_runner = XMLRunner() if irsx_flag else None
        
    yr_ind_new = pd.DataFrame()
    
    # Iterate through Object IDs
    for oid in oid_srch_lst:
        yr_ind_new = yr_ind_new.append( fetch_ind_row( irsx_flag, xml_runner, oid ) )
    
    yr_ind_new['990_SRC'] = "AWS FILE DIR"
    
    return yr_ind_new



#########################################
# MAIN
#########################################


# Iterate through all years
yr_lst = list( range( BEGIN_YR, END_YR + 1 ) )
for yr in yr_lst:
    
    # Read up-to-date index file if one exists, at time of writing 2009 and 2010 dont exist
    try:
        cur_ind_file = pd.read_csv( CUR_IND_FILE_PREF + str( yr ) + CUR_IND_FILE_SUFF )
        cur_ind_file['990_SRC'] = "AWS INDEX"
    except:
        cur_ind_file = pd.DataFrame( columns=[IND_FILE_OID_COL] )
    
    # Read new file list taken from aws_file_retrieval.py and make object ID column
    new_oid_file = pd.read_csv( NEW_OID_FILE_PREF + str( yr ) + NEW_OID_FILE_SUFF, usecols=[NEW_OID_FILE_COL] )
    new_oid_file[IND_FILE_OID_COL] = new_oid_file[NEW_OID_FILE_COL].str[:18]
    
    # Determine the object IDs to read
    oid_srch_lst = new_oid_file[~new_oid_file[IND_FILE_OID_COL].isin( cur_ind_file[IND_FILE_OID_COL] )]
    oid_srch_lst = oid_srch_lst[IND_FILE_OID_COL].tolist()
    
    print( "Read in " + str( yr ) + " files. Reading " + str( len( oid_srch_lst ) ) + " Object IDs." )
    
    # Fetch the information for the new index file and append it to the current one
    cur_ind_file = cur_ind_file.append( fetch_yr_ind( oid_srch_lst ) )
    
    # Save and print progress    
    cur_ind_file.to_csv( NEW_IND_FILE_PREF + str( yr ) + NEW_IND_FILE_SUFF, index=False )
    print( "Done with " + str( yr ) + " file." )
print( "Completed." )




