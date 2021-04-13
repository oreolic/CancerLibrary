#%%
from Executor import preprocessing as ppc
from Executor import rpm
from Executor import dataparsing as dp
import pandas as pd
import sys


#%%
def main(Data_combination):
    ppc.data_pre_processing(Data_combination)
    rpm.total_rpm().rpm_excutor(Data_combination)        
    return 

if __name__ == '__main__': 
    data_combination = [(sys.argv[1], sys.argv[2],sys.argv[3],sys.argv[4])]
    main(data_combination)



# %%
