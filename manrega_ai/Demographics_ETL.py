#*******************************************************************************#
#                Author : Saideshwar Kotha                                      # 
#                Modified By : Saideshwar Kotha                                 #
#                Created on : 05-05-2020                                        #
#                Updated on : 04-07-2020                                        #
#                Company : Arthashastra Intelligence                            #
#                Designation : SDE                                              #
#*******************************************************************************#

import pandas as pd
import numpy as np
from termcolor import colored
from elasticsearch import Elasticsearch


def extract_data(*arguments):
    
    '''
    *arguments - list of dataframes
    Check length of arguments then acts accordingly 
         if length equals 1 goes into if condition 
         if length more than 1 then goes into else condition    
    '''
    
    multiIndex = False
    singleIndex = False
    for each_df in arguments:
        
        if isinstance(each_df.index,pd.MultiIndex):
            multiIndex = True
        else:
            singleIndex = True
    
    arg_length = len(arguments) #Length of the arguments
    if multiIndex == True and singleIndex == False:
        
        if arg_length == 1:
            final_df = list(arguments)[0]
            if not final_df.index.names[0] == 'Frequency':
                final_df = final_df.swaplevel()   #Swapping the index levels
            final_df = final_df.astype('str')
            final_df = final_df.replace(to_replace = [',','.','-','}','{','-','_','*','@','$','**','',np.nan],value = '') #Finds the below list of values and replace with empty string
            final_df = final_df.replace(to_replace = [r'[*,*]',r'[.]\Z',r'[-]\Z'],value = ['','',''],regex=True)
            return final_df
        else:                                       
            list_of_df = []                            
            for df in arguments[1:]:            #Loop through the dataframes and call append method which modify to a single datafame
                list_of_df.append(df)
            final_df = arguments[0].append(list_of_df,ignore_index=False,sort=False)
            if not final_df.index.names[0] == 'Frequency':
                final_df = final_df.swaplevel()   #Swapping the index levels
            final_df = final_df.astype('str')

            final_df = final_df.replace(to_replace = [',','.','-','}','{','-','_','*','@','$','**','',np.nan],value = '') #Finds the below list of values and replace with empty string
            final_df = final_df.replace(to_replace = [r'[*,*]',r'[.]\Z',r'[-]\Z'],value = ['','',''],regex=True)
            return final_df
        
    elif multiIndex == False and singleIndex == True:
        
        if arg_length == 1:
            final_df = list(arguments)[0]
            final_df = final_df.astype('str')
            final_df = final_df.replace(to_replace = [',','.','-','}','{','-','_','*','@','$','**','',np.nan],value = '') #Finds the below list of values and replace with empty string
            final_df = final_df.replace(to_replace = [r'[*,*]',r'[.]\Z',r'[-]\Z'],value = ['','',''],regex=True)
            return final_df
        else:
            print(colored("For SingleIndex only one dataframe is allowed, but {} passed.",'red').format(arg_length))
            
    else:
        print(colored("All dataframes must either be SingleIndex or MultiIndex, shouldn't be both type.","red"))  
        
        
        
        

def transform_data(df,docSource):
    '''
    Checks datatype of each value of the key in the docSource variable and acts accordingly
    
    '''
    indexes = list(docSource.keys())
    meta_df = pd.DataFrame(columns=df.columns,index=indexes)
    for index in indexes:
        #Checking datatype of each index
        if isinstance(docSource[index],str):
            meta_df.loc[index] = docSource[index]
            
        elif isinstance(docSource[index],dict):
            if len(meta_df.columns) == len(docSource[index]):         #Checking length of dict to the number of columns
                for col in meta_df.columns:
                    meta_df[col][index] = docSource[index][col]
            else:
                print(colored('-'*70,'red'))
                print(colored('LengthError','red'),' '*25 ,'Traceback (most recent call last)')
                print('\n')
                print(colored('LengthError:','red'),'Length of the dictionary for "{}" is not equal to the length of the dataframe columns'.format(index))
                break
        else:
            meta_df.loc[index] = ''
            
    return meta_df






def load_data(df,meta_df,index_name,doc_type,uuid):

    elastic = Elasticsearch(['https://elastic.airesearch.in:443'])

    indexType = ''
    if isinstance(df.index,pd.MultiIndex):
        indexType = 'Multi'
    else:
        indexType = 'Single'
    
    if indexType == 'Multi':
        print("Multi")
        for column in df:
            DocSource = dict()

            meta_data = meta_df[column].to_dict()
            DocSource.update(meta_data)
            
            ##**************************************************#
            variable_path = meta_data['Path']+'/'+ column
           
            variable_path = variable_path.replace('//','/')
            
            DocSource.update({'VariablePath':variable_path})
            
            ##**************************************************#
            DocSource.update({ "VariableName": column  })
            
            ##**************************************************#
            path = variable_path
            level_hierarchy = dict()
            words = path.split("/") 
            count = 1
            for word in words:
                level_hierarchy['Level'+str(count)] = word
                count += 1
            level_hierarchy['Level_depth'] = count-1

            DocSource.update(level_hierarchy)

            
            Temp_df = df[[column]]
            data = dict()
            for index in Temp_df.index.levels[0]:

                Temp_df = Temp_df.loc[str(index)]
                Temp_df = Temp_df.rename(columns={column:"Variable"})
                Temp_df_JSON = Temp_df.to_dict()
               
                data.update( {index: Temp_df_JSON["Variable"]} )


            DocSource.update({"data":data})  

            ############################################
            response = elastic.index(index = index_name,doc_type = doc_type,id = uuid,body = DocSource)
            print('uuid is',uuid)
            uuid += 1

        return uuid
    
    elif indexType == 'Single':
        indexes = meta_df.index
        if 'Mode' in indexes:
            meta_data = meta_df[meta_df.columns[0]].to_dict()
            Mode = meta_data['Mode']
            
            #**********************************************************************#
            
            if Mode == '':
                print(colored("Mode should not be an empty string","red"))
            else:
                if 'State' in meta_data:
                    State = meta_data['State']
                else:
                    State = None

                if 'District' in meta_data:
                    District = meta_data['District']
                else:
                    District = None

                if 'SubDistrict' in meta_data:
                    SubDistrict = meta_data['SubDistrict']
                else:
                    SubDistrict = None

                if 'Ward' in meta_data:
                    Ward = meta_data['Ward']
                else:
                    Ward = None
                
                
                if Mode == 'State':
                    if State!=None or District!=None or SubDistrict!=None or Ward!=None:
                        print(colored('KeyError: If Mode = "State" then State,District,SubDistrict,Ward Keys should be None.','red'))
                        
                    else:
                        for column in df:
                            Doc_Source = dict()

                            meta_data = meta_df[column].to_dict()
                            Doc_Source.update(meta_data)

                            ##**************************************************#
                            variable_path = meta_data['Path']+'/'+ column 
                            variable_path  = variable_path.replace('//','/')
                            Doc_Source.update({'VariablePath':variable_path})
                            
                            ##**************************************************#
                            Doc_Source.update({ "VariableName": variable_path.split('/')[1]  })

                            ##**************************************************#
                            Doc_Source.update({ "Mode": Mode  })

                            ##**************************************************#
                            variable_name = { "State": column  }
                            Doc_Source.update(variable_name)

                            ##**************************************************#

                            level_hierarchy = {}
                            words = variable_path.split("/") 
                            count = 1
                            for word in words:
                                level_hierarchy['Level'+str(count)] = word
                                count += 1
                            level_hierarchy['Level_depth'] = count-1

                            Doc_Source.update(level_hierarchy)



                            ##**************************************************#
                            Temp_df = df[[column]]
                            Temp_df = Temp_df.rename(columns = { column:"Variable"} )
                            Temp_df_JSON = Temp_df.to_dict()

                            Doc_Source.update( {"data": Temp_df_JSON["Variable"] } )

                            ##**************************************************#
                            response = elastic.index(index = index_name, doc_type = doc_type, id = uuid, body = Doc_Source)
                            print('uuid is',uuid)
                            uuid += 1

                        return uuid
                elif Mode == 'District':
                    if not 'State' in meta_data:
                        print(colored("KeyError: If Mode = 'District' then, Key 'State' should exits.","red"))
                        
                    elif State == '':
                        print(colored("Key 'State' should not be an empty string.","red"))
                        
                    elif District!=None or SubDistrict!=None or Ward!=None:
                        print(colored('KeyError: If Mode = "District" then District, SubDistrict, Ward Keys should be None.','red'))
                        
                    else:
                        for column in df:
                            Doc_Source = dict()

                            meta_data = meta_df[column].to_dict()
                            Doc_Source.update(meta_data)

                            ##**************************************************#
                            variable_path = meta_data['Path']+ '/' + meta_data['State'] +'/'+ column  
                            variable_path  = variable_path.replace('//','/')
                            Doc_Source.update({'VariablePath':variable_path})
                            
                            ##**************************************************#
                            Doc_Source.update({ "VariableName": variable_path.split('/')[1]  })
                            
                            ##**************************************************#
                            Doc_Source.update( { "Mode": Mode  } )
                            Doc_Source.update( { "State": meta_data['State'] } )
                            Doc_Source.update( { "DistrictName": column } )
                            
                            ##**************************************************#
                            level_hierarchy = {}
                            words = variable_path.split("/") 
                            count = 1
                            for word in words:
                                level_hierarchy['Level'+str(count)] = word
                                count += 1
                            level_hierarchy['Level_depth'] = count-1

                            Doc_Source.update(level_hierarchy)

                            ##**************************************************#
                            Temp_df = df[[column]]
                            Temp_df = Temp_df.rename(columns = {column:"Variable"})
                            Temp_df_JSON = Temp_df.to_dict()

                            Doc_Source.update( {"data": Temp_df_JSON["Variable"]} )

                            ##**************************************************#
                            response = elastic.index(index = index_name, doc_type = doc_type, id = uuid, body = Doc_Source)
                            print('uuid is',uuid)
                            uuid += 1

                        return uuid    
                elif Mode == 'SubDistrict':
                    if not 'State' in meta_data:
                        print(colored("KeyError: If Mode = 'SubDistrict' then, Key 'State' should exits.","red"))
                        
                    elif State == '':
                        print(colored("Key 'State' should not be an empty string.","red"))
                        
                    elif not 'District' in meta_data:
                        print(colored("KeyError: If Mode = 'SubDistrict' then, Key 'District' should exits.","red"))
                        
                    elif District == '':
                        print(colored("Key 'District' should not be an empty string.","red"))
                        
                    elif SubDistrict!=None or Ward!=None:
                        print(colored('KeyError: If Mode = "SubDistrict" then SubDistrict,Ward Keys should be None.','red'))
                        
                    else:
                        for column in df:
                            Doc_Source = dict()

                            meta_data = meta_df[column].to_dict()
                            Doc_Source.update(meta_data)

                            ##**************************************************#
                            variable_path = meta_data['Path']+'/'+ meta_data['State'] +'/'+ meta_data['District'] +'/'+ column 
                            variable_path  = variable_path.replace('//','/')
                            Doc_Source.update({'VariablePath':variable_path})

                            ##**************************************************#
                            Doc_Source.update({ "VariableName": variable_path.split('/')[1]  })
                            
                            ##**************************************************#
                            Doc_Source.update({ "Mode": Mode  })
                            Doc_Source.update({ "State": meta_data['State']  })
                            Doc_Source.update({ "District": meta_data['District'] })
                            Doc_Source.update({ "SubDistrictName": column })
                            
                            ##**************************************************#
                            level_hierarchy = {}
                            words = variable_path.split("/") 
                            count = 1
                            for word in words:
                                level_hierarchy['Level'+str(count)] = word
                                count += 1
                            level_hierarchy['Level_depth'] = count-1

                            Doc_Source.update(level_hierarchy)

                            ##**************************************************#
                                                        
                            Temp_df = df[[column]]
                            Temp_df = Temp_df.rename(columns = {column:"Variable"})
                            Temp_df_JSON = Temp_df.to_dict()

                            Doc_Source.update( {"data": Temp_df_JSON["Variable"]} )

                            ##**************************************************#
                            response = elastic.index(index = index_name, doc_type = doc_type, id = uuid, body = Doc_Source)
                            print('uuid is',uuid)
                            uuid += 1

                        return uuid
                elif Mode == 'Ward':
                    if not 'State' in meta_data:
                        print(colored("KeyError: If Mode = Ward then, Key 'State' should exits.","red"))
                        
                    elif State == '':
                        print(colored("Key 'State' should not be an empty string.","red"))
                        
                    elif not 'District' in meta_data:
                        print(colored("KeyError: If Mode = Ward then, Key 'District' should exits.","red"))
                        
                    elif District == '':
                        print(colored("Key 'District' should not be an empty string.","red"))
                        
                    elif not 'SubDistrict' in meta_data:
                        print(colored("KeyError: If Mode = Ward then, Key 'SubDistrict' should exits.","red"))
                        
                    elif SubDistrict == '':
                        print(colored("Key 'SubDistrict' should not be an empty string.","red"))
                        
                    elif Ward!=None:
                        print(colored('KeyError: If Mode = "Ward" then Ward Keys should be None.','red'))
                        
                    else:
                        for column in df:
                            Doc_Source = dict()

                            meta_data = meta_df[column].to_dict()
                            Doc_Source.update(meta_data)

                            ##**************************************************#
                            variable_path = meta_data['Path']+'/'+ meta_data['State'] +'/'+ meta_data['District'] +'/'+  meta_data['SubDistrict'] +'/'+ column
                            variable_path  = variable_path.replace('//','/')
                            Doc_Source.update({'VariablePath':variable_path})

                            ##**************************************************#
                            Doc_Source.update({ "VariableName": variable_path.split('/')[1]  })
                            
                            ##**************************************************#

                            Doc_Source.update({ "Mode": Mode  })
                            Doc_Source.update({ "State": meta_data['State']  })
                            Doc_Source.update({ "District": meta_data['District'] })
                            Doc_Source.update({ "SubDistrict": meta_data['SubDistrict'] })
                            Doc_Source.update({ "WardName": column })
                            
                            ##**************************************************#
                            level_hierarchy = {}
                            words = variable_path.split("/") 
                            count = 1
                            for word in words:
                                level_hierarchy['Level'+str(count)] = word
                                count += 1
                            level_hierarchy['Level_depth'] = count-1

                            Doc_Source.update(level_hierarchy)

                            
                            ##**************************************************#
                            Temp_df = df[[column]]
                            Temp_df = Temp_df.rename(columns = {column:"Variable"})
                            Temp_df_JSON = Temp_df.to_dict()

                            Doc_Source.update( {"data": Temp_df_JSON["Variable"] } )

                            ##**************************************************#
                            response = elastic.index(index = index_name, doc_type = doc_type, id = uuid, body = Doc_Source)
                            print('uuid is',uuid)
                            uuid += 1

                        return uuid 
        else:
            print("Key 'Mode' is missing.")
                    
    else:
        print(colored('Dataframes are neither SingleIndex nor MultiIndex.','red'))