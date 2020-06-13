import urllib3
from bs4 import BeautifulSoup
import pandas as pd
import re
import traceback
import logging 
import sys
import os
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
http = urllib3.PoolManager()

# urls = ['https://www.malacards.org/categories/smell_taste_disease_list',
# 'https://www.malacards.org/categories/oral_disease_list',
# 'https://www.malacards.org/categories/neuronal_disease_list',
# 'https://www.malacards.org/categories/blood_disease_list',
# 'https://www.malacards.org/categories/liver_disease_list',
# 'https://www.malacards.org/categories/muscle_disease_list',
# 'https://www.malacards.org/categories/eye_disease_list',
# 'https://www.malacards.org/categories/bone_disease_list',
#urls = ['https://www.malacards.org/categories/skin_disease_list']
# 'https://www.malacards.org/categories/immune_disease_list',
# 'https://www.malacards.org/categories/endocrine_disease_list',
# 'https://www.malacards.org/categories/mental_disease_list',
# 'https://www.malacards.org/categories/respiratory_disease_list',
# 'https://www.malacards.org/categories/cardiovascular_disease_list',
# 'https://www.malacards.org/categories/nephrological_disease_list',
# 'https://www.malacards.org/categories/gastrointestinal_disease_list',
# 'https://www.malacards.org/categories/ear_disease_list',
# 'https://www.malacards.org/categories/reproductive_disease_list',
# ]

letters = sys.argv[1]
start = time.time()

# letters = '123456789abcdefghijklmnopqrstuvwxyz'
# letters='acdghklmnop'
# letters='z'
for i in letters:
    letter=i
    # print("letter", letter)

    folderPath = letter + "_disease"
# folderPath='missing_disease'

    if not os.path.exists(folderPath):
        os.makedirs(folderPath)

    url = 'https://www.malacards.org/malalist/' + letter + '?showAll=true'
    logging.basicConfig(filename='scraper.log',filemode='w' ,format='%(asctime)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S', level = logging.DEBUG)

    try:
        links=[]
        

        # logging.debug(str('Scraping data from: '+url))  
        HEADER ={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}
        #file_name = url.split('/')[-1].replace('_disease_list','')
        file_name = letter + "_disease/" + letter
        # file_name = 'missing_disease/'+'missing'
        res = http.request('GET', url,headers=HEADER)
        soup = BeautifulSoup(res.data, 'html.parser')

        # links = ["google.com"]

        table = soup.find("table", attrs={'style':'width:50%;margin:auto 0;position:relative;'})
        cnt = 0
        t = []
        temp = []
        

        mcid = []
        name = []
        alias = []
        category = []
        external_id = []
        summary = []
        family_diseases = []
        related_diseases = []
        comorbidal_diseases = []
        umls_symptoms = []
        hpo_phenotypes = []
        omim_symptoms = []
        GenomeRNAi_Phenotypes =[]
        MGI_MOUSE_Phenotypes = []
        drugs = []
        clinical_trial = []
        gene_tests = []
        related_genes= []
        clinvar = []
        uniProt_KB_variations = []
        cnvd = []
        pathway = []
        cellular_components = []
        biological_process = []
        molecular_fucntions = []
        
        
        family=[]
        parent_names=[]
        
        family_data=[]
        
        links=[]
        
        
        # mcids_scrap=[
        #     'ADN016',
        #     'ALZ034',
        #     'ANR040',
        #     'ATM095',
        #     'CRB039',
        #     'CNG034',
        #     'DRM006',
        #     'DBT009',
        #     'GST053',
        #     'HRT032',
        #     'HPT023',
        #     'HMN044',
        #     'HYP056',
        #     'KDN018',
        #     'LKM002',
        #     'LNG032',
        #     'LYM118',
        #     'MCH012',
        #     'MCH014',
        #     'NRP001',
        #     'NTR004',
        #     'OVR042',
        #     'PRS040']
        link_list=[]
        for row in table.find_all('tr'):
            t=[]
            for td in row.find_all('td'):
                t.append(td.getText().replace('\n',''))
                # print(td,end='\t')
            try:
                temp.append(t) 
                # print(td.find('a').get('href'))
                link_list.append(td.find('a').get('href'))
                # print(row)
            except Exception as e:
                print('',end='')
            # if cnt<2:
            #     t.append(row.get_text().replace('\n',''))
            #     cnt+=1
            # else:
            #     cnt = 0
            #     t.append(row.get_text().replace('\n',''))
            #     temp.append(t)
            #     t = []
            #print(temp)
        for link in link_list:
            # link_text = link.get('href')
            # link_text = "https://www.malacards.org/card/"+mcid.lower()
            link_text ="https://www.malacards.org"+link
            
            # "https://www.malacards.org/card/diabetes_mellitus?
            # link_text+="?limit[RelatedGenes]=1000000&limit[RelatedDiseases]=1000000#RelatedDiseases-table&limit[Comorbidity]=1000000#Comorbidity-table&limit[go_proc]=1000000#go_proc-table&limit[ClinicalTrial]=1000000#ClinicalTrial-table&limit[ClinVarVariations]=1000000#ClinVarVariations-table&limit[Comorbidity]=1000000#Comorbidity-table&limit[CnvdVariations]=1000000#CnvdVariations-table&limit[MaladiesUnifiedCompounds]=1000000#therapeutics&limit[Pathway]=1000000#Pathway-table&limit[RelatedDiseases]=1000000#RelatedDiseases-table"
            
            # print('link ', link_text)
            
            
            # if (links[-1] != link_text):
            links.append(link_text)

        # links = links[1:]
        # print(links)
        
        if links==[]:
            raise Exception('No links to scrap in letter '+letter)
        else:
            print('letter ',letter,' links',links)

        df = pd.DataFrame(data=temp)  
        df.columns = ['Name','MCID','Alias']
        df['anatomical_categoty'] = file_name
        df.to_csv(file_name,sep='\t')
        df.to_json(str(file_name+'_json'),orient='records') 
        counter=1
        total=len(links)
        
        for link in links:
            logging.debug('Current link: '+link)
            print('link {} of {} '.format(counter,total)+'Current link: '+link)
            counter+=1
            # print(link)

            open_internal_link = http.request('GET',link)
            # print(open_internal_link)
            internal_soup = BeautifulSoup(open_internal_link.data,'html.parser')
            

            #get id of disease
            id_div = internal_soup.find('div',{'class':'symbol'})
            mc_id = 'None_mcid'
            if id_div is not None:
                mc_id = id_div.get_text().replace('MCID: ','')
                mcid.append(mc_id)
            # else:
            #     mcid.append(mc_id)

            if mc_id!='None_mcid':

                #get names of diseases
                main_name_tag=internal_soup.find('div',{'class':'main-name'})
                main_name = 'None'
                if main_name_tag is not None:
                    main_name = main_name_tag.find('b').get_text()
                    name.append(main_name)
                else:
                    name.append(main_name)

                #get aliases of disease
                alias_table = internal_soup.find('table',{'class':'borderless aliases','style':''})
                cnt = 0
                for div in alias_table.find_all('div'):
                    cnt += 1
                    t= []
                    t.extend((mc_id,div.get_text().strip().split('\r')[0]))
                    alias.append(t)
                if cnt == 0:
                    t = []
                    t.extend((mc_id,'None'))
                    alias.append(t)

                
                # To split and get categories
                # document.getElementsByClassName("card-description")[0].innerHTML.split(":")[1].trim().split('</d')[0].replace(/[^a-zA-Z0-9]/g, '').split('diseases')

                # get categories of disease
                category_table = internal_soup.find('div', {'class': 'card-description'})
                cnt = 0
                for div in category_table.find_all('div'):
                    cnt += 1
                    t = []
                    categories = div.get_text().split(":")[1].strip()
                    categoriesList = ''.join(e for e in categories if e.isalnum())
                    categories = categoriesList.split('diseases')
                    cat_str = ','.join(categories)
                    #print(cat_str)
                    t.extend((mc_id, cat_str))
                    category.append(t)
                if (cnt == 0):
                    t = []
                    t.extend((mc_id, 'None'))
                    category.append(t)
                t = []
                #print(category)
                    
                # get external ids of disease
                external_ids = internal_soup.find('div',{'id':'ExternalId'})
                id_name_dict = {}
                for td in external_ids.find_all('b'):
                    id_name_dict.update({td.parent.get('id'):td.get_text()})

                id_value_dict = {}
                for td in external_ids.find_all(['a','span']):
                    temp =[]
                    text_value = td.get_text()
                    if td.parent.name == "sup" or text_value == '\nmore\n' or text_value == 'less' or text_value == 'more' or td.parent.get('id') == None:
                        pass
                    else:
                        if '\n' in text_value:
                            text_value = text_value.replace('\n',',')[1:-1]
                        temp.append(text_value)
                        if td.parent.get('id') in id_value_dict:
                            id_value_dict[td.parent.get('id')].append(text_value)
                        else:
                            id_value_dict.update({td.parent.get('id'):temp})
                
                temp = {}
                for k in id_value_dict:
                    if len(id_value_dict[k]) > 1:
                        tstr = ','.join(id_value_dict[k])
                        id_value_dict.update({k:tstr.split(',')})
                        temp.update({id_name_dict[k]:id_value_dict[k]})
                        
                    else:
                        temp.update({id_name_dict[k]:id_value_dict[k]})
                if temp not in external_id:
                    temp.update({'MCID':mc_id})
                    external_id.append(temp)

                #get summary of diseases
                summary_div = internal_soup.find('div',{'id':'Summary'})
                if summary_div is not None:
                    summary.append(summary_div.get_text().replace('\n','').replace('\r','').replace('  ',''))
                else:
                    summary.append('None')
            
                #get family diseases
                # find_name = main_name.replace(' ','_').lower()
                # dis_name =None
                # family_div = internal_soup.find_all('a',href=True)
                # for a in family_div:
                #     if a['href'] == '/card/{}'.format(find_name):
                #         dis_name=a.get_text()
                # if dis_name==main_name:
                #     family_table = internal_soup.find('table',{'class':'family borderless'})
                #     if family_table is not None:
                #         for td in family_table.find_all('td'):
                #             t=[]
                #             family_text=td.get_text()
                #             if '\n' in family_text:
                #                 family_text = family_text.replace('\n','')
                #             t.extend((mc_id,family_text))
                #             family_diseases.append(t)
                #     else:
                #         t=[]
                #         t.extend((mc_id,'None'))
                #         family_diseases.append(t)
                        
                        
                
                family_div = internal_soup.find_all('a',href=True)
                
                # main_name = internal_soup.body.find_all(text='Diseases in the')
                # print('name',main_name)
                parent_name=''
                for h3 in internal_soup.find_all('h3'):
                    if 'Diseases in the' in h3.getText():
                            # print(i)
                            # print(i.find('a'))
                            # print(i.find('a').getText())
                            parent_name=h3.find('a').getText()  
                
                
                if parent_name!='':
                    # print('parent_name',parent_name)
                    # for a in family_div:
                        # if a['href'] == '/card/{}'.format(find_name):
                        #     dis_name=a.get_text()
                    if parent_name not in parent_names:
                        family_table = internal_soup.find('table',{'class':'family borderless'})
                        if family_table is not None:
                            child=[]
                            for td in family_table.find_all('td'):
                                t=[]
                                family_text=td.get_text()
                                if '\n' in family_text:
                                    family_text = family_text.replace('\n','')
                                child.append(family_text)
                                
                                # t.extend((mc_id,family_text))
                                # family_diseases.append(t)
                            parent_names.append(parent_name)
                            family.append({parent_name:child})
                        # else:
                        #     t=[]
                        #     t.extend((mc_id,'None'))
                        #     family_diseases.append(t)
                    # print('family',family)
                
                for parent_list in family:
                    for parent in parent_list.keys():
                        parent_name=parent
                    # parent_name = parent_list.keys()
                    # print(parent_name)
                    
                    child_list=parent_list[parent_name]
                    
                    for child in child_list:
                        family_data.append((parent_name,child))
            
                #get related diseases
                
                open_internal_link = http.request('GET',link+'?limit[RelatedDiseases]=1000000#RelatedDiseases-table')
                internal_soup1 = BeautifulSoup(open_internal_link.data,'html.parser')
            
                related_disease_table = internal_soup1.find('table',{'id':'RelatedDiseases-table'})
                t= []
                if related_disease_table is not None:
                    t.append(mc_id)
                    cntt = 0
                    for td in related_disease_table.find_all('td'):
                        re_dis_text = td.get_text()
                        if '\n' in re_dis_text:
                            re_dis_text = re_dis_text.replace('\n',',')
                            re_dis_text = re_dis_text[1:-1]
                        if cntt<3:
                            t.append(re_dis_text)
                            cntt+=1
                        else:
                            cntt=0
                            t.append(re_dis_text)
                            related_diseases.append(t)
                            t = []
                            t.append(mc_id)
                else:
                    t = []
                    t.extend((mc_id,'None','None','None','None'))
                    related_diseases.append(t)

                open_internal_link = http.request('GET',link+'?limit[Comorbidity]=1000000#Comorbidity-table')
                internal_soup2 = BeautifulSoup(open_internal_link.data,'html.parser')
            
                #get comorbidal disease in related diseases section
                comorbidal_diseases_table = internal_soup2.find('table',{'id':'Comorbidity-table'})
                if comorbidal_diseases_table is not None:
                    for td in comorbidal_diseases_table.find_all('td'):
                        t = []
                        t.extend((mc_id,td.get_text().replace('\n','')))
                        comorbidal_diseases.append(t)
                else:
                    t = []
                    t.extend((mc_id,'None'))
                    comorbidal_diseases.append(t)
                
                #get umls symptoms in symptoms & phenotypes section
                symptoms_div = internal_soup.find('div',{'class':'symptoms_div'})
                if symptoms_div is not None:
                    for span in symptoms_div.find_all('span'):
                        t= []
                        span_text = span.get_text().replace('\n','').replace(',','')
                        if span_text not in t:
                            if '\r' not in span_text:
                                t.extend((mc_id,span_text))
                                if t not in umls_symptoms:
                                    umls_symptoms.append(t)
                            else:
                                t.extend((mc_id,'None'))
                                if t not in umls_symptoms:
                                    umls_symptoms.append(t)
                else:
                    t= []
                    t.extend((mc_id,'None'))
                    umls_symptoms.append(t)
                # print(umls_symptoms)

                #get hpo_phenotypes in symptoms & phenotypes section
                hpo_phenotypes_table = internal_soup.find('table',{'id':'HPO_Symptoms-table'})
                if hpo_phenotypes_table is not None:
                    t = []
                    t.append(mc_id)
                    for td in hpo_phenotypes_table.find_all('td'):
                        hpo_text = td.get_text()
                        if '\n\r\n' in hpo_text:
                            hpo_text = hpo_text.replace('\n\r\n','').replace('  ','').split('\n')[0].replace('\r','')
                        if '\n' in hpo_text:
                            hpo_text = hpo_text.replace('\n','').replace('  ','')
                        if '<a' not in str(td):
                            t.append(hpo_text)
                        else:
                            if len(t) == 4:
                                t.append('None')
                            t.append(hpo_text)
                            hpo_phenotypes.append(t)
                            t = []
                            t.append(mc_id)
                else:
                    t = []
                    t.extend((mc_id,'None','None','None','None','None'))
                    hpo_phenotypes.append(t)

                # #get omim symptoms in symptoms & phenotypes section
                omim_symptoms_div = internal_soup.find('div',{'id':'Symptoms'})
                if omim_symptoms_div is not None:
                    for div in omim_symptoms_div.find_all('span'):
                        t=[]
                        omim_symp_text=div.get_text()
                        if '\n' in omim_symp_text:
                            omim_symp_text=omim_symp_text.replace('\n','').replace('\r','').replace('  ','')
                        if 'showing' not in omim_symp_text:
                            t.extend((mc_id,omim_symp_text))
                            omim_symptoms.append(t)
                        #else:
                        #    t=[]
                        #    t.extend((mc_id,'None'))
                        #    omim_symptoms.append(t)        
                else:
                    t=[]
                    t.extend((mc_id,'None'))
                    omim_symptoms.append(t)
                    
                # print(omim_symptoms)

                #get GenomeRNAi_Phenotypes in symptoms & phenotypes section
                functional_description_GEN_table = internal_soup.find('table',{'id':'FunctionalDescription_GEN-table'})
                if functional_description_GEN_table is not None:
                    t= []
                    t.append(mc_id)
                    cnt=0
                    for td in functional_description_GEN_table.find_all('td'):
                        GEN_text = td.get_text()
                        if '\n' in GEN_text:
                            GEN_text = GEN_text.replace('\n',',')[1:-1]
                        if cnt<4:
                            t.append(GEN_text)
                            cnt+=1
                        else:
                            t.append(GEN_text)
                            cnt=0
                            GenomeRNAi_Phenotypes.append(t)
                            t = []
                            t.append(mc_id)
                else:
                    t= []
                    t.extend((mc_id,'None','None','None','None','None'))
                    GenomeRNAi_Phenotypes.append(t)

                #get MGI Mouse Phenotypes in symptoms & phenotypes section
                functional_description_MGI_table = internal_soup.find('table',{'id':'FunctionalDescription_MGI-table'})
                if functional_description_MGI_table is not None:
                    t= []
                    t.append(mc_id)
                    cnt=0
                    for td in functional_description_MGI_table.find_all('td'):
                        MGI_text = td.get_text()
                        if '\n' in MGI_text:
                            MGI_text = MGI_text.replace('\n',',')[1:-1]
                        if cnt<4:
                            t.append(MGI_text)
                            cnt+=1
                        else:
                            t.append(MGI_text)
                            cnt=0
                            MGI_MOUSE_Phenotypes.append(t)
                            t = []
                            t.append(mc_id)
                else:
                    t= []
                    t.extend((mc_id,'None','None','None','None','None'))
                    MGI_MOUSE_Phenotypes.append(t)

                open_internal_link = http.request('GET',link+'?limit[MaladiesUnifiedCompounds]=1000000#MaladiesUnifiedCompounds-table')
                internal_soup3 = BeautifulSoup(open_internal_link.data,'html.parser')
            
                #get Drugss in Drugs & Therapeutics section
                drugs_table = internal_soup3.find('table',{'id':'MaladiesUnifiedCompounds-table'})
                t=[]
                if drugs_table is not None:
                    cnt=0
                    t.append(mc_id)
                    for td in drugs_table.find_all('td'):
                        drug_text = td.get_text()
                        td_width = td.get('width')
                        if td_width is None and td.get('colspan') is None:
                            if cnt<7:
                                t.append(drug_text)
                                cnt+=1
                            else:
                                t.append(drug_text)
                                drugs.append(t)
                                t =[]
                                cnt=0
                                t.append(mc_id)
                else:
                    t.extend((mc_id,'None','None','None','None','None','None','None','None'))
                    drugs.append(t)
            
                open_internal_link = http.request('GET',link+'?limit[ClinicalTrial]=1000000#ClinicalTrial-table')
                internal_soup4 = BeautifulSoup(open_internal_link.data,'html.parser')
                
                #get Clinical Trial in Drugs & Therapeutics section
                clinical_trial_table = internal_soup4.find('table',{'id':'ClinicalTrial-table'})
                if clinical_trial_table is not None:
                    cnt = 0
                    t = []
                    t.append(mc_id)
                    for td in clinical_trial_table.find_all('td'):
                        trial_text = td.get_text()
                        if '\n' in trial_text:
                            trial_text = trial_text.replace('\n','')[1:-1]
                        if cnt<5:
                            t.append(trial_text)
                            cnt+=1
                        else:
                            cnt=0
                            t.append(trial_text)
                            clinical_trial.append(t)
                            t = []
                            t.append(mc_id)
                else:
                    t = []
                    t.extend((mc_id,'None','None','None','None','None','None'))
                    clinical_trial.append(t)
                
                #get genetic tests in genetic tests section
                gene_test_table = internal_soup.find('table',{'id':'gene_tests'})
                t = []
                if gene_test_table is not None:
                    cnt = 0
                    t.append(mc_id)
                    for td in gene_test_table.find_all('td'):
                        gene_test_text = td.get_text()
                        if '\r\n' in gene_test_text:
                            gene_test_text = gene_test_text.split('\r\n')[1].replace('  ','')
                        if '\n' in gene_test_text:
                            gene_test_text =gene_test_text.replace('\n',',')[1:-1]
                        if cnt<2:
                            t.append(gene_test_text) 
                            cnt+=+1
                        else:
                            t.append(gene_test_text)
                            gene_tests.append(t)
                            cnt = 0
                            t = []
                            t.append(mc_id)
                else:
                    t.extend((mc_id,'None','None','None'))
                    gene_tests.append(t)
                    
                    
                open_internal_link = http.request('GET',link+'?limit[RelatedGenes]=1000000#RelatedGenes-table')
                internal_soup5 = BeautifulSoup(open_internal_link.data,'html.parser')
                
                #get related genes in genes section
                genes_table = internal_soup5.find('table',{'id':'RelatedGenes-table'})
                t= []
                if genes_table is not None:
                    cnt = 0
                    t.append(mc_id)
                    for td in genes_table.find_all('td'):
                        if cnt==1:
                            ELITE_GENE=0
                            try:
                                if (td.find('div',{'class':'asterisk_icon2'})):
                                    ELITE_GENE=1
                            except Exception as e:
                                pass
                        gene_text = td.get_text()
                        if '\r' in gene_text:
                            gene_text = gene_text.replace('  ','').replace('\n\n','').replace('\r','').split('(show sections)')[0].replace('\n\n',',')[1:-1].replace('\n',',').replace('(more)','')
                        if '\n\n' in gene_text:
                            gene_text = gene_text.replace('\n\n','').replace('\n','')
                        if '\n' in gene_text:
                            cnt+=1
                        if cnt<6:
                            t.append(gene_text)
                            cnt+=1
                        else:
                            t.append(gene_text)
                            flag=0
                            for s in t:
                                if '\n' in s:
                                    flag=1
                                    break
                            if flag == 0:
                                t.append(ELITE_GENE)
                                related_genes.append(t)      
                            t =[]
                            cnt =0
                            t.append(mc_id)       
                else:
                    t.extend((mc_id,'None','None','None','None','None','None','None','None'))
                    related_genes.append(t)

                open_internal_link = http.request('GET',link+'?limit[ClinVarVariations]=1000000#ClinVarVariations-table')
                internal_soup6 = BeautifulSoup(open_internal_link.data,'html.parser')
                
                #get clinvar in variations section
                clinvar_table = internal_soup6.find('table',{'id':'ClinVarVariations-table'})
                t =[]
                if clinvar_table is not None:
                    cnt=0
                    t.append(mc_id)
                    for td in clinvar_table.find_all('td'):
                        clinvar_text = td.get_text()
                        if '\n' in clinvar_text:
                            clinvar_text = clinvar_text.replace('\n',' ')[1:-1]
                        if cnt<7:
                            t.append(clinvar_text)
                            cnt+=1
                        else:
                            t.append(clinvar_text)
                            clinvar.append(t)
                            cnt=0
                            t = []
                            t.append(mc_id)
                else:
                    t.extend((mc_id,'None','None','None','None','None','None','None','None'))
                    clinvar.append(t)

                uni_port_kb_variations_table = internal_soup.find('table',{'id':'geneticVariations-table'})
                t= []
                if uni_port_kb_variations_table is not None:
                    cnt = 0
                    t.append(mc_id)
                    for td in uni_port_kb_variations_table.find_all('td'):
                        var_text = td.get_text()
                        if '\n' in var_text:
                            var_text = var_text.replace('\n','').replace(' ','')
                        if cnt<4:
                            t.append(var_text)
                            cnt+=1
                        else:
                            t.append(var_text)
                            uniProt_KB_variations.append(t)      
                            t =[]
                            cnt =0
                            t.append(mc_id)         
                else:
                    t.extend((mc_id,'None','None','None','None','None'))
                    uniProt_KB_variations.append(t)

                open_internal_link = http.request('GET',link+'?limit[ClinVarVariations]=1000000#ClinVarVariations-table')
                internal_soup9 = BeautifulSoup(open_internal_link.data,'html.parser')
                #get cnvd in variations section
                copy_number_variations_table = internal_soup9.find('table',{'id':'CnvdVariations-table'})
                t=[]
                if copy_number_variations_table is not None:
                    cnt=0
                    t.append(mc_id)
                    for td in copy_number_variations_table.find_all('td'):
                        cnvd_text = td.get_text()
                        if '\n' in cnvd_text:
                            cnvd_text = cnvd_text.replace('\n',' ')[1:-1]
                        if cnt<7:
                            t.append(cnvd_text)
                            cnt+=1
                        else:
                            t.append(cnvd_text)
                            cnvd.append(t)
                            cnt=0
                            t=[]
                            t.append(mc_id)
                else:
                    t.extend((mc_id,'None','None','None','None','None','None','None','None'))
                    cnvd.append(t)
                
                open_internal_link = http.request('GET',link+'?limit[Pathway]=1000000#Pathway-table')
                internal_soup7 = BeautifulSoup(open_internal_link.data,'html.parser')
                
                #get pathways in pathway section
                pathway_table = internal_soup7.find('table',{'id':'Pathway-table'})
                t =[]
                if pathway_table is not None:
                    cnt=0
                    t.append(mc_id)
                    for td in pathway_table.find_all('td'):
                        path_text = td.get_text()
                        if 'Show member pathways' in path_text:
                            path_text = path_text.split('Show member pathways')[0].replace('\n\n','').split('\n')[0]
                        if '\n\n' in path_text:
                            path_text = path_text.replace('\n\n','').split('\n')[0]
                        if '\n' in path_text:
                            path_text = path_text.replace('\n',',')[1:-1]
                        if cnt<3:
                            t.append(path_text)
                            cnt+=1
                        else:
                            t.append(path_text)
                            pathway.append(t)
                            cnt=0
                            t=[]
                            t.append(mc_id)
                else:
                    t.extend((mc_id,'None','None','None','None'))
                    pathway.append(t)

                #get cellular components in go terms
                cellular_components_table = internal_soup.find('table',{'id':'go_cc-table'})
                t= []
                if cellular_components_table is not None:
                    cnt=0
                    t.append(mc_id)
                    for td in cellular_components_table.find_all('td'):
                        cell_text = td.get_text()
                        if '\n' in cell_text:
                            cell_text = cell_text.replace('\n',',')[1:-1]
                        if cnt<4:
                            t.append(cell_text)
                            cnt+=1
                        else:
                            t.append(cell_text)
                            cellular_components.append(t)
                            t = []
                            cnt=0
                            t.append(mc_id)
                else:
                    t.extend((mc_id,'None','None','None','None','None'))
                    cellular_components.append(t)

                open_internal_link = http.request('GET',link+'?limit[go_proc]=1000000#go_proc-table')
                internal_soup8 = BeautifulSoup(open_internal_link.data,'html.parser')
                
                #get biological process in go terms
                biological_processes_table = internal_soup8.find('table',{'id':'go_proc-table'})
                t = []
                if biological_processes_table is not None:
                    cnt=0
                    t.append(mc_id)
                    for td in biological_processes_table.find_all('td'):
                        bio_text = td.get_text()
                        if '\n' in bio_text:
                            bio_text =bio_text.replace('\n',',')[1:-1]
                        if cnt<4:
                            t.append(bio_text)
                            cnt+=1
                        else:
                            t.append(bio_text)
                            biological_process.append(t)
                            t=[]
                            cnt=0
                            t.append(mc_id)  
                else:
                    t.extend((mc_id,'None','None','None','None','None'))
                    biological_process.append(t)

                #get molecular functions  in go terms
                molecular_functions_table = internal_soup.find('table',{'id':'go_func-table'})
                t=[]
                if molecular_functions_table is not None:
                    cnt=0
                    t.append(mc_id)
                    for td in molecular_functions_table.find_all('td'):
                        molecular_text = td.get_text()
                        if '\n' in molecular_text:
                            molecular_text = molecular_text.replace('\n',',')[1:-1]
                        if cnt<4:
                            t.append(molecular_text)
                            cnt+=1
                        else:
                            t.append(molecular_text)
                            cnt=0
                            molecular_fucntions.append(t)
                            t=[]
                            t.append(mc_id)
                else:
                    t.extend((mc_id,'None','None','None','None','None'))
                    molecular_fucntions.append(t)
                #break     
                
            
        family_data=[]


        df_basic = pd.DataFrame({
            'mcid': mcid,
            'disease_name': name,
            'summary': summary,
        })
        df_basic.to_csv(str(file_name+'_basic_info'),sep='\t')
        #df_basic.to_json(str(file_name+'_basic_info_json'),orient='records') 

        df_alias = pd.DataFrame(data=alias)
        df_alias.columns = ['mcid','alias']
        df_alias.to_csv(str(file_name+'_aliases'),sep='\t')
        #df_alias.to_json(str(file_name+'_aliases_json'),orient='records') 
        
        df_category = pd.DataFrame(data=category)
        df_category.columns = ['mcid', 'categories']
        df_category.to_csv(str(file_name+'_categories'),sep='\t')
        #df_category.to_json(str(file_name+'_categories_json'),orient='records') 

        df_external_ids = pd.DataFrame(data = external_id)
        df_external_ids.to_csv(str(file_name+'_external_ids'),sep='\t')
        #df_external_ids.to_json(str(file_name+'_external_ids_json'),orient='records') 

        # df_family_diseases = pd.DataFrame(data=family_diseases)
        # df_family_diseases.columns = ['mcid','child']
        # df_family_diseases.to_csv(str(file_name+'_family_diseases'),sep='\t')
        # #df_family_diseases.to_json(str(file_name+'_family_diseases_json'),orient='records') 
        

        df_related_diseases = pd.DataFrame(data=related_diseases)
        df_related_diseases.columns = ['mcid','#','Related Disease','Score','Top Affiliating Genes']
        df_related_diseases.to_csv(str(file_name+'_related_diseases'),sep='\t')  
        #df_related_diseases.to_json(str(file_name+'_related_diseases_json'),orient='records')     

        df_comorbidal_diseases = pd.DataFrame(data=comorbidal_diseases)
        df_comorbidal_diseases.columns = ['mcid','Comorbidity relations ']
        df_comorbidal_diseases.to_csv(str(file_name+'_comorbidal_diseases'),sep='\t')
        #df_comorbidal_diseases.to_json(str(file_name+'_comorbidal_diseases_json'),orient='records')

        if umls_symptoms!=[]:
            df_umls_symptoms = pd.DataFrame(data = umls_symptoms)
            df_umls_symptoms.columns = ['mcid','UMLS symptoms']
            df_umls_symptoms.to_csv(str(file_name+'_umls_symptoms'),sep='\t')
        #df_umls_symptoms.to_json(str(file_name+'_umls_symptoms_json'),orient='records') 

        df_hpo_phenotypes = pd.DataFrame(data = hpo_phenotypes)
        df_hpo_phenotypes.columns = ['mcid','#','Description','HPO Frequency','Orphanet Frequency','HPO Source Accession',]
        df_hpo_phenotypes.to_csv(str(file_name+'_hpo_phenotypes'),sep='\t') 
        #df_hpo_phenotypes.to_json(str(file_name+'_hpo_phenotypes_json'),orient='records')

        #print(omim_symptoms)
        if omim_symptoms!=[]:
                
            df_omim_symptoms = pd.DataFrame(data = omim_symptoms)
            df_omim_symptoms.columns = ['mcid','OMIM Symptom']
            df_omim_symptoms.to_csv(str(file_name+'_omim_symptoms'),sep='\t') 
            #df_omim_symptoms.to_json(str(file_name+'_omim_symptoms_json'),orient='records')

        df_GenomeRNAi_Phenotypes = pd.DataFrame(data=GenomeRNAi_Phenotypes)
        df_GenomeRNAi_Phenotypes.columns = ['mcid','#','Description','GenomeRNAi Source Accession','Score','Top Affiliating Genes']
        df_GenomeRNAi_Phenotypes.to_csv(str(file_name+'_GenomeRNAi_Phenotypes'),sep='\t') 
        #df_GenomeRNAi_Phenotypes.to_json(str(file_name+'_GenomeRNAi_Phenotypes_json'),orient='records')

        df_MGI_MOUSE_Phenotypes = pd.DataFrame(data=MGI_MOUSE_Phenotypes)
        df_MGI_MOUSE_Phenotypes.columns = ['mcid','#','Description','MGI Source Accession','Score','Top Affiliating Genes']
        df_MGI_MOUSE_Phenotypes.to_csv(str(file_name+'_MGI_MOUSE_Phenotypes'),sep='\t')  
        #df_MGI_MOUSE_Phenotypes.to_json(str(file_name+'_MGI_MOUSE_Phenotypes_json'),orient='records')
        
        df_drugs = pd.DataFrame(data=drugs)
        df_drugs.columns = ['mcid','#','++','Name','Status','Phase','Clinical Trials','Cas Number','PubChem Id']
        df_drugs.to_csv(str(file_name+'_drugs'),sep='\t') 
        #df_drugs.to_json(str(file_name+'_drugs_json'),orient='records')

        df_clinical_trial = pd.DataFrame(data=clinical_trial)
        df_clinical_trial.columns = ['mcid','#','Name','Status','NCT ID','Phase','Drugs']
        df_clinical_trial.to_csv(str(file_name+'_clinical_trial'),sep='\t') 
        #df_clinical_trial.to_json(str(file_name+'_clinical_trial_json'),orient='records')  

        df_gene_tests = pd.DataFrame(data=gene_tests)
        df_gene_tests.columns = ['mcid','#','Genetic test','Affiliating Genes']
        df_gene_tests.to_csv(str(file_name+'_gene_tests'),sep='\t') 
        #df_gene_tests.to_json(str(file_name+'_gene_tests_json'),orient='records')

        df_related_genes = pd.DataFrame(data=related_genes)
        df_related_genes.columns = ['mcid','#','Symbol','Description','Category','Score','Evidence','PubmedIds','isElite']
        df_related_genes.to_csv(str(file_name+'_related_genes'),sep='\t') 
        #df_related_genes.to_json(str(file_name+'_related_genes_json'),orient='records')

        df_clinvar = pd.DataFrame(data=clinvar)
        df_clinvar.columns = ['mcid','#','Gene','Variation','Type','Significance','SNP ID','Assembly','Location']
        df_clinvar.to_csv(str(file_name+'_clinvar'),sep='\t')
        #df_clinvar.to_json(str(file_name+'_clinvar_json'),orient='records')

        df_uniProt_KB_variations = pd.DataFrame(data=uniProt_KB_variations)
        df_uniProt_KB_variations.columns = ['mcid','#','Symbol','AA change','Variation ID','SNP ID']
        df_uniProt_KB_variations.to_csv(str(file_name+'_uniProt_KB_variations'),sep='\t') 
        #df_uniProt_KB_variations.to_json(str(file_name+'_uniProt_KB_variations_json'),orient='records')

        df_cnvd = pd.DataFrame(data=cnvd)
        df_cnvd.columns = ['mcid','#','CNVD ID','Chromosom','Start','End','Type','Gene Symbol','CNVD Disease']
        df_cnvd.to_csv(str(file_name+'_cnvd'),sep='\t') 
        #df_cnvd.to_json(str(file_name+'_cnvd_json'),orient='records')

        df_pathway = pd.DataFrame(data=pathway)
        df_pathway.columns = ['mcid','#','Super pathways','Score','Top Affiliating Genes']
        df_pathway.to_csv(str(file_name+'_pathway'),sep='\t') 
        #df_pathway.to_json(str(file_name+'_pathway_json'),orient='records')

        df_cellular_components = pd.DataFrame(data=cellular_components)
        df_cellular_components.columns = ['mcid','#','Name','GO ID','Score','Top Affiliating Genes']
        df_cellular_components.to_csv(str(file_name+'_cellular_components'),sep='\t')
        #df_cellular_components.to_json(str(file_name+'_cellular_components_json'),orient='records') 

        df_biological_process = pd.DataFrame(data=biological_process)
        df_biological_process.columns = ['mcid','#','Name','GO ID','Score','Top Affiliating Genes']
        df_biological_process.to_csv(str(file_name+'_biological_process'),sep='\t')
        #df_biological_process.to_json(str(file_name+'_biological_process_json'),orient='records') 

        df_molecular_fucntions = pd.DataFrame(data=molecular_fucntions)
        df_molecular_fucntions.columns = ['mcid','#','Name','GO ID','Score','Top Affiliating Genes']
        df_molecular_fucntions.to_csv(str(file_name+'_molecular_fucntions'),sep='\t') 
        #df_molecular_fucntions.to_json(str(file_name+'_molecular_fucntions_json'),orient='records') 

        if family_data !=[]:
            df_family_diseases = pd.DataFrame(data=family_data)
            df_family_diseases.columns = ['Parent','child']
            df_family_diseases.to_csv(str(file_name+'_family_diseases'),sep='\t')
            df_family_diseases.to_json(str(file_name+'_family_diseases_json'),orient='records')       
        
        
    except Exception as e:
        logging.error("Exception Occured in method",exc_info=True) 

end = time.time()

print(end - start,' seconds')