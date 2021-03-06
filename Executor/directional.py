#%%
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime



# %%

class directional:

    def _make_every_ed1_seq (self,rnd_barcode):
        base = ['A', 'T', 'G', 'C']
        ed1_list = []
        for i in range(len(rnd_barcode)):
            if i == 0:
                for nt in base:
                    ed1 = nt + rnd_barcode[1:]
                    if ed1 == rnd_barcode:
                        pass
                    else:
                        ed1_list.append(ed1)
            elif i == len(rnd_barcode) - 1:
                for nt in base:
                    ed1 = rnd_barcode[:-1] + nt
                    if ed1 == rnd_barcode:
                        pass
                    else:
                        ed1_list.append(ed1)
            else:
                for nt in base:
                    ed1 = rnd_barcode[:i] + nt + rnd_barcode[i + 1:]
                    if ed1 == rnd_barcode:
                        pass
                    else:
                        ed1_list.append(ed1)

        return ed1_list


    def _possible_ed1(self, df):
        rnd_lst = list(df.iloc[:,1])
        rnd_dic = dict(zip(df.iloc[:,1],df.iloc[:,2]))

        tuplist = []
        final = []
        for rnd in rnd_lst:
            if type(rnd) != str:                
                pass
            else:            
                if rnd.find('N') != -1:                    
                    pass
                else:
                    if type(rnd) != str:                        
                        pass
                    else:
                        possible_ed1 = directional()._make_every_ed1_seq(rnd)
                        true_ed1     = [i for i in possible_ed1 if i in rnd_lst]
                        if len(true_ed1) == 0:
                            final.append((rnd,rnd_dic[rnd]))
                        else:
                            for k in true_ed1:
                                tuplist.append([(rnd,rnd_dic[rnd]),(k,rnd_dic[k])])
        
        ## Final: Rnd without ED1 in the total Rnd List
        ## tupList: Rnd tup list with ED1 in the total Rnd List
        return tuplist, final

    def compare_count(self,a, b):

        if a >= 3 * b - 1:
            return 1
        else:
            return 0

    def _same_count(self, ed1tuplist):
        lst = []
        for tup in ed1tuplist:
            if tup[0][1] == tup[1][1]:
                lst.extend(tup)
            else:
                pass
        lst = list(set(lst))
        return lst

    def _ed1_group(self, possible_ed1):
        ## possibleED1 = return of _Possible_ED1
        ed1tuplist = possible_ed1[0]
        final = possible_ed1[1] 

        true_ed1 = [ tup for tup in ed1tuplist if directional().compare_count(tup[0][1],tup[1][1]) == 1]
        same = self._same_count(ed1tuplist)
        

        ## True Ed1 Re-Grouping 
        connode_dic = {}
        for tup in true_ed1:
            if tup[1] not in connode_dic:
                connode_dic[tup[1]] = [tup[0]]
            else:
                connode_dic[tup[1]].append(tup[0])

        for con in connode_dic:
            mainlst = connode_dic[con]
            if len(mainlst) ==1:
                pass
            else:
                mainlst.sort(key = lambda x:-x[1]) ##Rnd ?????????
                true_main = mainlst[0]
                connode_dic[con] = [true_main]
        
        dic = {}
        for tup in connode_dic:
            mainnode =connode_dic[tup][0]
            if mainnode not in dic:
                dic[mainnode] = [tup]
            else:
                dic[mainnode].append(tup)

        for tup in connode_dic:
            if tup not in dic:
                pass
            else:
                mainnode = connode_dic[tup][0]
                dic[mainnode].extend(dic[tup])

        allcons = []
        for tup in dic:
            tuplist = dic[tup]
            allcons.extend(tuplist)
            if len(tuplist) == 1:
                pass
            else:
                for p in tuplist:
                    if p not in dic:
                        pass
                    else:
                        dic[tup].extend(dic[p])
            dic[tup] = list(set(dic[tup]))
            
        allCons = list(set(allcons))

        finaldic = {}
        for tup in dic:
            if tup not in allcons:
                finaldic[tup] = dic[tup]
            else:
                pass


        dic = finaldic
        overlap = []
        for t in dic:
            if t not in same:
                pass
            else:
                overlap.append(t)
                
            for k in dic[t]:        
                if k not in same:
                    pass
                else:
                    overlap.append(k)

        truesame = [i for i in same if i not in overlap]

        for tup in truesame:
             final.append(tup)

        
        for tup in dic:
            read = tup[1]+sum([i[1] for i in dic[tup]])
            final.append((tup[0],read))

        df = pd.DataFrame(final)
        return df


    def _directional_adjacency(self, eachdf):
        sb = eachdf.iloc[0,0]
        possible_ed1 = self._possible_ed1(eachdf)
        final_rnd   = self._ed1_group(possible_ed1)
        
        final_rnd['Sorting_barcode'] = sb
        final_rnd = final_rnd[['Sorting_barcode',0,1]]
        return final_rnd


    def multi_directional_adjacency(self, dflist):
        t1 = datetime.now()
        with ProcessPoolExecutor(max_workers=25) as executor:
            futs = []

            for eachdf in dflist:
                fut = executor.submit(self._directional_adjacency, eachdf)
                futs.append(fut)

            merged = []
            for fut in futs:
                merged.append(fut.result())
            final_pds = pd.concat(merged)
            t2 = datetime.now()
            print('Directional_Adjacency: ', t2-t1)
            return final_pds



# %%
