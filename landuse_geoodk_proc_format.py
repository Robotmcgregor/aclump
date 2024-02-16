#!/usr/bin/env python

"""
Read in all the nt land use field data collected on either the BU or TD forms on the GeoODK survey app
and process and produce a single csv file and ESRI shapefile. 

Note: this script needs to read in the csv file titled 'ACLUM_codesV8_MasterList.csv' which should be located 
in the same directory as this script.

Author: grant.staben@nt.gov.au
Date: 30/03/2017

"""


import argparse
import pandas as pd
import math
import numpy as np
from random import sample
#import matplotlib.pyplot as plt
import csv
import glob
import os
import sys
import geopandas
from shapely.geometry import Point
import pdb


def getCmdargs():
    
    """input the command line arguments"""

    p = argparse.ArgumentParser()
    p.add_argument("--directory", help="Path of the directory with the GeoODK survey forms")
    p.add_argument("--csv", help="Name of the csv file")
    p.add_argument("--shp", help="Name of the esri shapefile")
    cmdargs = p.parse_args()
    # if there is no directory the script will terminate
    if cmdargs.directory is None:
        p.print_help()
        sys.exit()

    return cmdargs


def filestoProcess(path):
    
    """function to generate a list of files to process"""
    
    direc = path
    # file pattern to search for all GeoODK forms.
    pattern = "%sNT LAND USE FIELD DATA*" % direc
    # return a list of files matching the pattern "csv" in the directry 
    filelist = glob.glob(pattern)
    
    return filelist

def processFormBU(form):
    
    """Read in the BU GeoODK form and output the relevent attributes"""
                
    df = pd.read_csv(form, header=0)# reads the csv file 
    # select out the relevent fields from the BU survey form.
    n_df = df[['Collector', 'Collector_other', 'date_time','landuse_coord-Latitude', 'landuse_coord-Longitude', 'Landuse_photo', 
               'commod_yes-type_class', 'commod_yes-commodity_class', 'commod_yes-tertiary_class', 'commod_yes-secondary_class', 
               'commod_yes-primary_class', 'commod_yes-mangoes_variety-mango_varK', 'commod_yes-mangoes_variety-mango_vars', 
               'commod_yes-mangoes_variety-mango_vars_other', 'commod_yes-man_desc', 'commod_yes-man_type', 'commod_yes-man_sec', 
               'commod_yes-man_class', 'commod_yes-man_unk', 'commod_yes-man_unk_other', 'confidence', 'addit_comments', 
               'meta-instanceID', ]]
    
    formdata = []
        
    # loop over all the attributes in the df and select out the relevent fields and populate the empty lists
    for index, row in n_df.iterrows():
                  
        """extract the name of the collectors from the predefined or other option attributes
        to add to a empty list which is converted to a string for each record"""
    
        collList = [] # create an empty list to take in the collectors name
    
        ncol = str(row['Collector'])
        if ncol != 'nan':            
            collList.append(ncol)
        ncol2 = str(row['Collector_other']) 
        if ncol2 != 'nan':
            collList.append(ncol2)
        other = 'other'
        if other in collList: collList.remove(other)
        colle = str(collList)
        colle2 = colle.strip("[]")
     
        # extract each date from the date time record and append it to a new list.
        ndate = str(row['date_time'])
        ndate2 = ndate[:10]
        nlat = row['landuse_coord-Latitude']
        nlong = row['landuse_coord-Longitude']
        nphoto = row['Landuse_photo']
        ncomb = row['commod_yes-commodity_class']
        nprim = row['commod_yes-primary_class']
        nsec = row['commod_yes-secondary_class']
        ntert = row['commod_yes-tertiary_class'] 
        
        # read in the management fields and create a string
        manList = []
        man = str(row['commod_yes-man_class'])
        if man != 'nan':
            manList.append(man)
        man2 = str(row['commod_yes-man_unk_other'])
        if man2 != 'nan':
            manList.append(man)
        other = 'other'
        if other in manList: manList.remove(other)
        nan = 'nan'
        if nan in manList: manList.remove(nan)
        manLs = str(manList)
        manL = manLs.strip("[]")
        
        """Read in Gregs Mango variety fields and write out only the varieties - needs to account for NaN values
        some where in here it is reading in the 'other' when it reads in the mango variety field"""
     
        VarList = [] # create an empyt list to take in the varitey options
        nvar = str(row['commod_yes-mangoes_variety-mango_vars'])
        if nvar != 'nan':
            VarList.append(nvar)
        nvarO = str(row['commod_yes-mangoes_variety-mango_vars_other'])
        if nvarO != 'nan':
            VarList.append(nvarO)
        other = 'other'
        if other in VarList: VarList.remove(other)
        varity2 = str(VarList) 
        varity = varity2.strip("[]")
       
        n_conf = (row['confidence'])
        n_comm = str(row['addit_comments'])
        n_key = (row['meta-instanceID'])
        
        # output all the attributes for each row in a newlist
        newlist = [colle2.strip("'"),ndate2,nlat,nlong,nphoto,nprim,nsec,ntert,ncomb,varity.strip("'"),manL.strip("'",),n_conf,n_comm.strip("[]"),n_key]
        # append the new row to the list
        formdata.append(newlist)
    
    return formdata

def processFormTD(form):
    
    """Read in the TD GeoODK form and output the relevent attributes"""
    
    # read in the NT LAND USE FIELD DATA BU from and output the header row
    df = pd.read_csv(form, header=0)
    # select out the relevent fields from the TD survey form.
    n_df = df[['Collector', 'Collector_other', 'date_time','landuse_coord-Latitude', 'landuse_coord-Longitude',
               'Landuse_photo','confidence', 'addit_comments', 'start_primary-prim_class', 'start_primary-sec_class',
               'start_primary-tert_class', 'start_primary-commod_class', 'start_primary-commod_class_other',
               'start_primary-man_class_td', 'start_primary-man_class_td_other','meta-instanceID', ]]

    formdata = []
    
    # loop over all the attributes in the df and select out the relevent fields and populate the empty lists
    for index, row in n_df.iterrows():

        """extract the name of the collectors from the predefined or other option attributes
        to add to a empty list which is converted to a string for each record"""
    
        collList = [] # create an empyt list to take in the collectors name
        ncol = str(row['Collector'])
        if ncol != 'nan':
            collList.append(ncol)
        ncol2 = str(row['Collector_other'])
        if ncol2 != 'nan':
            collList.append(ncol2)
        other = 'other'
        if other in collList: collList.remove(other)
        colle = str(collList)
        colle2 = colle.strip("[]")
     
        # extract each date from the date time record and append it to a new list.
        ndate = row['date_time'][:10]
        nlat = row['landuse_coord-Latitude']
        nlong = row['landuse_coord-Longitude']
        nphoto = row['Landuse_photo']
        nprim = row['start_primary-prim_class']
        nsec = row[ 'start_primary-sec_class']
        ntert = row['start_primary-tert_class'] 
        ncomb = row['start_primary-commod_class_other']
        variety = '' # this creates a column with nan so we can append the two differenct survey forms to create a single csv file. 
        manL = str(row['start_primary-man_class_td_other'])
        n_conf = (row['confidence'])
        n_comm = str(row['addit_comments'])
        n_key = (row['meta-instanceID'])
        
        # output all the attributes for each row in a newlist
        newlist = [colle2.strip("'"),ndate,nlat,nlong,nphoto,nprim,nsec,ntert,ncomb,variety,manL.strip("'"),n_conf,n_comm.strip("[]"),n_key]
        # append each row to a list
        formdata.append(newlist)
        
    return formdata
    
def modifyaddFields(output):
    
    """Function to format the db codes output from GeoODK"""
    
    headers = ['Collectors','Date','Lat', 'Long','photo','Primary_db','Secondary_db','Tertiary_db',
               'Commodity_db','Variety_db','Management_db','Confidence_db','Comments','UIDkey']
    df = pd.DataFrame.from_records(output,columns=headers)
    
    def changePrim(df):
        
        """
        function to generate new numeric fields and change the primary, secondary and tertirary 
        fields from the database text format; 
    
        Conservation and Natural Environments = conservation_and_natural_envir'
        Production from Relatively Natural Environments = 'prod_from_rel_natural_environ'
        Production from Dryland Agriculture and Plantations = 'prod_dryland_agri_plant' 
        Production from Dryland Agriculture and Plantations = 'prod_from_dryland_agri_plant'
        Production from Irrigated Agriculture and Plantations = 'prod_irrigated_agri_plant'
        Production from Irrigated Agriculture and Plantations = 'production_from_irrigated_agriculture_and_plantations'
        Intensive Uses = intensive_uses 
        Water = water
    
        """
        if (df[['Primary_db']] == 'conservation_and_natural_envir').all():
            return "Conservation and Natural Environments"
        if (df[['Primary_db']] == 'prod_from_rel_natural_environ').all():
            return "Production from Relatively Natural Environments"
        if (df[['Primary_db']] == 'prod_dryland_agri_plant').all():
            return "Production from Dryland Agriculture and Plantations"
        if (df[['Primary_db']] == 'prod_from_dryland_agri_plant').all():
            return "Production from Dryland Agriculture and Plantations"
        if (df[['Primary_db']] == 'prod_irrigated_agri_plant').all():
            return "Production from Irrigated Agriculture and Plantations"
        if (df[['Primary_db']] == 'production_from_irrigated_agriculture_and_plantations').all():
            return "Production from Irrigated Agriculture and Plantations"
        if (df[['Primary_db']] == 'intensive_uses').all():
            return "Intensive Uses"
        if (df[['Primary_db']] == 'water').all():
            return "Water"
        else:
            return df
    
    df['Primary'] = df.apply(changePrim,axis=1)
    
    
    
    # replace the space in the text with an underscore 
    df['Secondary_l'] = df['Secondary_db'].str.replace('_',' ')    
    df['Tertiary_l'] = df['Tertiary_db'].str.replace('_',' ')
    df['Commodity_l'] = df['Commodity_db'].str.replace('_',' ')
    df['Variety_l'] = df['Variety_db'].str.replace('_',' ')
    df['Management_l'] = df['Management_db'].str.replace('_',' ')
    df['Confidence_l'] = df['Confidence_db'].str.replace('_',' ')
    
    # capitalise the first letter in each field
    df['Secondary'] = df.Secondary_l.str.capitalize()
    df['Tertiary'] = df.Tertiary_l.str.capitalize()
    df['Commodity'] = df.Commodity_l.str.capitalize()
    df['Variety'] = df.Variety_l.str.capitalize()
    df['Management'] = df.Management_l.str.capitalize()
    df['Confidence'] = df.Confidence_l.str.capitalize()
    
    # here we deal with the fact that there is no tertiary class for the grazing native vegetation
    df.loc[df.Secondary == 'Grazing native vegetation', 'Tertiary'] = 'Grazing native vegetation'
    
    # return the clean data as a pandas df
    clean_data = df[['Collectors','Date','Lat', 'Long','photo','Primary','Secondary','Tertiary','Commodity',
              'Variety','Management','Confidence','Comments','UIDkey']]
    
    return clean_data

def applyNumericCode(clean_data, csvName, scriptPath):
    
    """function to create the numeric codes
    The csv field containing all the aclum codes could be replaced with a  into the script 
    using the df.loc method in line 263"""
    aclump_codes = scriptPath + 'ACLUM_codesV8_MasterList.csv'   
    #df_list = pd.read_csv('C:\Users\grants\code\aclump\ACLUM_codesV8_MasterList.csv', header=0)
    df_list = pd.read_csv(aclump_codes, header=0)
    # read in and create a df output from the modifyaddFields function 
        
    headers = ['Collectors','Date','Lat', 'Long', 'photo','Primary','Secondary','Tertiary','Commodity','Variety','Management','Confidence','Comments','UIDkey']
    
    geoodk_output = pd.DataFrame.from_records(clean_data,columns=headers)
    
    # merge the two df using the tertiary column and remove the records with no spatial information.
    
    joinData = pd.merge(geoodk_output, df_list, how='outer', on='Tertiary')
    joinData.shape
    
    cleanD1 = joinData[(joinData['Lat'] <= 0)] # if it does not have a coordinate it is ignored 
    cleanD = cleanD1[(cleanD1['Secondary'] != "")]
   
    # output the clean data to a csv file 
    cleanD[['Collectors','Date','Lat', 'Long','photo','Primary','Secondary','alum_v8','lu_code','lu_coden','Commodity','Variety','Management','Confidence','Comments','UIDkey']]
    
    cleanD.rename(columns={'alum_v8':'Tertiary'})
 
    #output all of the merged data as csv file so checks can be undertaken 
    cleanD.to_csv(csvName)
            
def outputshp(csvName,shapeName):
    
    """
    Function to read in the csv file 
    and output an esri shapefile
    """
       
    df = pd.read_csv(csvName, header=0)
    
    df['geometry'] = [Point(xy) for xy in zip(df['Long'], df['Lat'])]
    # create a df with all of the attributes 
    df2 = df[['geometry','Collectors','Date','Lat', 'Long','photo','Primary','Secondary','Tertiary','lu_code','lu_coden','Commodity','Variety','Management','Confidence','Comments','UIDkey']]
    # create the geo data frame        
    df2 = geopandas.GeoDataFrame(df2, geometry='geometry')
    # set the projection and datum
    df2.crs = "+init=epsg:4326"
    # save out the geo data frame in esri shapefile format.
    df2.to_file(shapeName, driver='ESRI Shapefile')    
        
def mainRoutine():
    
    cmdargs = getCmdargs() 
    # empty list to append the processed geoodk records
    output = []
    # path to the directory containing the individual csv files
    path = cmdargs.directory
    
    #pdb.set_trace()
    
    # get the path to the directory this script is running from - this is added to the csv file containing all the aclump codes
    scriptpath = str(sys.argv[0])
    scriptPath = scriptpath[:-29]
    
    pdb.set_trace()
    
    filelist = filestoProcess(path)
    
    for form in filelist:
        form
        pathL = len(path) # get the lenght of the directory path to remove it from the file name
        formN = form[pathL:]
        formtype = formN[25:27]
          
        # deal with the fact that there are two differnent types of field forms that need to be treated differently. 
        if formtype == 'BU':
            formdata = processFormBU(form)
            for row in formdata:
                output.append(row)
            print ('Form being processed ',form)
            print ("----------------------------------------------------------------------------")
   
        elif formtype == 'TD':
            formdata = processFormTD(form)
            for row in formdata:
                output.append(row)           
            print ('Form being processed ',form)
            print ("----------------------------------------------------------------------------")
            
    clean_data = modifyaddFields(output)
         
    csvName = cmdargs.csv
        
    applyNumericCode(clean_data, csvName, scriptPath)

    shapeName = cmdargs.shp
    
    outputshp(csvName,shapeName)
    
if __name__ == "__main__":
    mainRoutine()