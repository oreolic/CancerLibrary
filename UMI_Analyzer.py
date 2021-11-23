#%%
from Executor import Preprocessing as ppc
from Executor import RPM as rpm
import pandas as pd
import sys

#%%
def main(Data_combination):
    ppc.data_preprocessing(Data_combination)
    rpm.TotalRPM().calculate_rpm(Data_combination)        
    return 

if __name__ == '__main__': 
    data_combination = [(sys.argv[1], sys.argv[2],sys.argv[3],sys.argv[4])]
    main(data_combination)



    