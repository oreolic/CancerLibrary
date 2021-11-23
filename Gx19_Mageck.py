#%%
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from Executor import DataParsing as DP
import os
import sys
import random

def rpm(df):
    tot10 = df['D10_Count'].sum()
    tot24 = df['D24_Count'].sum()

    rpm10 = lambda x:x/tot10*1e6
    rpm24 = lambda x:x/tot24*1e6

    df['D10_RPM'] = df['D10_Count'].apply(rpm10)+1
    df['D24_RPM'] = df['D24_Count'].apply(rpm24)+1

    return df 

def make_subgroup(eachdf,n_group):
    if eachdf.shape[0] >=50:
        pass
    else:
        return pd.DataFrame()
    if n_group == 1:
        eachdf['group'] = 1
    else:
        eachdf['group'] = ['{}'.format(random.randrange(1,n_group+1)+i-i) for i in range(eachdf.shape[0])]
    lst = []
    sb = eachdf.iloc[0,0]
    for idx in list(set(eachdf['group'])):
        subgroup = eachdf[eachdf['group']==idx]
        d10 = subgroup['D10_Count'].sum()
        d24 = subgroup['D24_Count'].sum()
        lst.append([sb,idx,d10,d24])
    eachdf = pd.DataFrame(lst,columns = ['Sorting_Barcode','Group','D10_Count','D24_Count'])
    return eachdf

#%%

def make_input(dataset,rep,num_subgroup):
    df = pd.read_csv('Result/{}_{}_Processed_Count.txt'.format(dataset,rep),sep='\t')
    df.columns = ['Sorting_Barcode','RandomBarcode','D10_Count','D24_Count']    
    bc = pd.read_csv('Input/Barcode/{}_Barcode.txt'.format(dataset),sep='\t',index_col=0)

    dfdic = DP.Data_Parsing().dfdic(df)

    gx_dic = {}
    for sb in dfdic:
        if sb not in list(bc.index):
            pass
        else:
            gx19 = bc.loc[sb,'GX19']
            if gx19 not in gx_dic:
                gx_dic[gx19] = [dfdic[sb]]
            else:
                gx_dic[gx19].append(dfdic[sb])

    with ProcessPoolExecutor(max_workers=30) as executor:
        futs = []
        for sb in gx_dic:
            eachlst = gx_dic[sb]
            
            eachdf = pd.concat(eachlst)
            eachdf['Sorting_Barcode'] = sb.upper()

            fut = executor.submit(make_subgroup,eachdf,num_subgroup)
            futs.append(fut)
        final = []
        for fut in futs:
            eachdf = fut.result()
            if eachdf.shape[0] ==0:
                pass
            else:
                eachdf.columns = ['GX19','Group','D10_Count','D24_Count']
                final.append(eachdf)


        final = pd.concat(final)
        final = rpm(final)

        final = final[['Group','GX19','D10_RPM','D24_RPM']]
        final.columns = ['sgRNA','Sorting_Barcode','D10_RPM','D24_RPM']
        
        sgrna = ['RND{}'.format(i) for i in range(final.shape[0])]
        final['sgRNA'] = sgrna
        final.to_csv('GX19_Mageck/{}_{}_GX19_Mageck_input.txt'.format(dataset,rep),sep='\t',index=None)

        return final

    
def main(dataset,rep,groupnum):
    make_input(dataset,rep,groupnum)
    os.system('mageck test -k ./GX19_Mageck/{a}_{b}_GX19_Mageck_input.txt'.format(a = dataset,b = rep,c= groupnum)+
        ' -c 0 -t 1 --norm-method median'+' -n GX19_Mageck/'+
        '{a}_{b}_GX19_Mageck_{c}_group'.format(a = dataset,b = rep,c = groupnum))
        
    return

# %%
if __name__ == '__main__':
    pass
    for dataset in ['24K']:
        for rep in ['R1','R2']:
            for num in [4]:
                main(dataset,rep,num)

    ## argv[1] = dataset
    ## argv[2] = replicate
    ## argv[3] = number_group

    ##main(sys.argv[1],sys.argv[2],sys.argv[3])
#%%%
a = os.listdir('./GX19_Mageck')

for i in a:
    if i.find('gene') != -1:
        pass
    elif i.find('input') != -1:
        pass
    else:
        os.system('rm ./GX19_Mageck/{}'.format(i))

# %%
