import json
import os
import codecs
import pandas as pd
import numpy as np
import string
import xml.etree.ElementTree as ET
import re 

def get_var_custom(query="BRAC2,SEAC5,SEAC_ST,UC,calc2",label="Данные",name="calculation",uid="ff760b74-f5c8-4935-a467-655d48f3e022"):
    false=False
    soptions=query.split(',')
    options=[]
    for option in soptions:
        options.append(       
              {
                "selected": false,
                "text": option,
                "value": option
              })
    
    out={
            "current": {
              "selected": false,
              "text": "UC",
              "value": "UC"
            },
            "hide": 0,
            "includeAll": false,
            "label": label,
            "multi": false,
            "name": name,
            "options": options,
            "query": ',\n'.join(soptions),
            "queryValue": soptions[0],
            "skipUrlSync": false,
            "type": "custom"
    }
    return out

def get_var(query="SHOW MEASUREMENTS",label="Данные",name="calculation",uid="ff760b74-f5c8-4935-a467-655d48f3e022"):
    null=None
    false=False
    true=True
    u_list={
        "current": {
          "selected": false,
          "text": "Analise",
          "value": "Analise"
        },
        "datasource": {
          "type": "influxdb",
          "uid": uid
        },
        "definition": query,
        "hide": 0,
        "includeAll": false,
        "label": label,
        "multi": false,
        "name": name,
        "options": [],
        "query": query,
        "refresh": 1,
        "regex": "",
        "skipUrlSync": false,
        "sort": 0,
        "type": "query"
      }
    return u_list

def templating_list(path2file='.\Grafana\Boilernaya\Boilernaja.xlsx',sheet_name='Vars'):
    dataVars=pd.read_excel(path2file,sheet_name=sheet_name)
    l=[]
    for i in range(dataVars.shape[0]):
        temp=dataVars.iloc[i]
        if 'Custom' in temp.type:
            l.append(get_var_custom(query=temp['query'],label=temp['label'],name=temp['name']))
        if 'Query' in temp.type:
            l.append(get_var(query=temp['query'],label=temp['label'],name=temp['name']))
            
    return {"list":l}

def num2alfabeta(i):
    if i<26:
        out=string.ascii_uppercase[i] 
    else:
        out=string.ascii_uppercase[int(np.fix(i/26-1))]+string.ascii_uppercase[i%26] 
    return  out
# Формирование Json для запроса к базе

def get_query2(fname='КА11.D0L2',refId='A',var='typecalc'):
    if var in ['typecalc']:
        # Тип запроса 1
        #query=f'SELECT mean(\"{fname}\") FROM /^$calculation$/ WHERE  (\"Station\"::tag =~/^$station$/) AND (\"Equipment\"::tag =~/^$equipment$/) AND (\"TypeCalc\"::tag =~/^$'+var+'$/) AND $timeFilter GROUP BY time($__interval) fill(none)'
        query=f'SELECT mean(\"{fname}\") FROM /^$calculation$/ WHERE ("Ni"::tag =~ /^$Ni$/ AND "fleet"::tag =~ /^$fleet$/) AND ("n_boilers"::tag =~ /^$nBoilers$/) AND $timeFilter GROUP BY time($__interval) fill(none)'
    else:    
        # Тип запроса 2
        
        query=f'SELECT mean(\"{fname}\") FROM /^$calculation$/ WHERE  (\"Station\"::tag =~/^$station$/) AND (\"Equipment\"::tag =~/^$equipment$/) AND (\"TypeCalc\"::tag =~/^$'+var+'$/) AND ("Model"::tag =~/^$model$/) AND ("Scenario"::tag =~/^$scenario$/) AND ("Version"::tag =~/^$version$/) AND $timeFilter GROUP BY time($__interval) fill(none)'
    
    temp={'alias': fname.replace('.','_'),
     'groupBy': [{'params': ['$__interval'], 'type': 'time'},
      {'params': ['null'], 'type': 'fill'}],
     'orderByTime': 'ASC',
     'policy': 'default',
     'query': query,
     'rawQuery': True,
     'refId': refId,
     'resultFormat': 'time_series',
     'select': [[{'params': ['value'], 'type': 'field'},
        {'params': [], 'type': 'mean'}]],
     'tags': [{'key': 'name', 'operator': '=', 'value': fname}]}
    return temp

# Формирование Json для заполнения цветом и стрелочек
def get_rools(fname='КА11.D0',Shape='',Text='',Add_Text='anl'):
    aliace=fname.replace('.','_')
    temp={'aggregation': 'current',
     'alias': aliace,                      # Заменяется
     'colors': ['#3274D9', '#56A64B', '#FF780A', '#E02F44'],
     'column': 'Time',
     'dateFormat': 'YYYY-MM-DD HH:mm:ss',
     'decimals': 2,
     'eventData': [],
     'eventProp': 'id',
     'eventRegEx': False,
     'gradient': False,
     'hidden': False,
     'invert': False,
     'linkData': [],
     'linkProp': 'id',
     'linkRegEx': True,
     'mappingType': 1,
     'metricType': 'serie',
     'order': 1,
     'overlayIcon': False,
     'pattern': aliace,                     # Заменяется
     'rangeData': [],
     'reduce': True,
     'refId': 'A',
     'sanitize': False,
     'shapeData': [{'colorOn': 'a',
       'hidden': False,
       'pattern': Shape ,                    # Заменяется
       'style': 'fillColor'}],
     'shapeProp': 'id',
     'shapeRegEx': True,
     'stringThresholds': ['/.*/'],
     'textData': [{'hidden': False,
       'pattern': Text,                      # Заменяется
       'textOn': 'wmd',
       'textPattern': '/.*/',
       'textReplace': Add_Text}],            # Заменяется
     'textProp': 'id',
     'textRegEx': True,
     'thresholds': [0, 20, 100],
     'tooltip': False,
     'tooltipColors': False,
     'tooltipLabel': '',
     'tooltipOn': 'a',
     'tpDirection': 'v',
     'tpGraph': False,
     'tpGraphHigh': None,
     'tpGraphLow': None,
     'tpGraphScale': 'linear',
     'tpGraphSize': '100%',
     'tpGraphType': 'line',
     'type': 'number',
     'unit': 'short',
     'valueData': []}
    return temp

def generate_targets(DataFile,sheet_name='Правила'):
    #import copy
    data=pd.read_excel(DataFile,sheet_name=sheet_name)
    data_t=data[['Переменная']]
    data_t=data_t.rename(columns = {'Переменная':'value'})
    out=[]
    for i, fname in enumerate(data_t.value):
        out.append(get_query2(fname,num2alfabeta(i),var='typecalc'))

def generate_rulesData(DataFile,sheet_name='Правила'):
    # Берём тэги из Файла:
    data=pd.read_excel(DataFile,sheet_name=sheet_name)
    # Формирование правил данных и добаление из в json
    out=[]
    for i in data.index:
        fname=data['Переменная'][i]
        shape_color=data['Цвет'][i]
        for id_cell_column in ['Показывать значение','id']:
            if  id_cell_column in data.keys():
                text=data[id_cell_column][i]
            
        add_text=data['Добалвение текста'][i]
        print(i,fname,shape_color,text)
        out.append(get_rools(fname,shape_color,text,add_text))

# Забираем BinaryJson из grafana
def get_B_drawio_xml_string(JsonFile):
    with codecs.open(JsonFile, "r","utf-8") as json_file:
        data_j=json.load(json_file)
    BinaryJson=data_j['panels'][0]['flowchartsData']['flowcharts'][0]['xml']
    return BinaryJson 

# Форомирование таблицы с данными из json DrawIO Grafana

# Сохранение таблицы с переменными DrawIO
def Draio2Table(final_xml_string,Prefix=''):
    if len(final_xml_string)<100:
        DrawIOFile=final_xml_string
        tree = ET.parse(DrawIOFile)
        root = tree.getroot()
    else:    
        root = ET.fromstring(final_xml_string)
    diagram = root[0]
    Table=[]
    for mxCell in diagram.iter('mxCell'):
        id=mxCell.get('id')
        if mxCell.get('value') is None:
            Value=''
        else:
            Value=mxCell.get('value')
            Value=Value.replace('&nbsp;','')
            out=re.search('>\w*\s?\w*</',Value)
            if not out is None:
                Value=out.group(0)[1:-2]
        if mxCell.get('style') is None:
            figure=''
        else:
            figure=mxCell.get('style').split(';')[0]
        if len(Value)>0:   
            Table.append(pd.DataFrame({'id':[id],'Переменная':[Prefix+Value],'Добалвение текста':['anl'],'Цвет':[' '],'Тип фигуры':[figure]}))
    Table=pd.concat(Table)
    Table=Table.reset_index().drop(columns=['index'])
    return  Table

def correct_Gr_Json(JsonFile,DataFile,DrawIO,Type=2):
    # Чтение данных модели
    with codecs.open(JsonFile, "r","utf_8_sig") as json_file:
        data_j=json.load(json_file)

    # Чтение шаблона
    out=generate_rulesData(DataFile,sheet_name='Правила')
    data_j['panels'][0]['rulesData']['rulesData']=out
    
    out=generate_targets(DataFile,sheet_name='Правила')
    data_j['panels'][0]['targets']=out
    
    # Проверяем присутствие переменных:
    if len(data_j['templating']['list'])>0:
        data_j['templating']=templating_list(path2file=DataFile)
        
    # Данные по таблице
    if len(data_j['panels'])>1:
        data_j['panels'][1]['targets']=out
        # Доделать!
        #data_j['panels'][1]['transformations']=get_transformations()
    
    # Обновляем DrawIO
    if len(DrawIO)<100:
        tree = ET.parse(DrawIO)
        root = tree.getroot()
        text = root[0].text
    else:    
        text = DrawIO
        
        
    # Обновление схемы
    data_j['panels'][0]['flowchartsData']['flowcharts'][0]['xml']=text


    # Сохранение результата
    with open(JsonFile[:-4]+str(Type)+"__correct.json","w", encoding='utf-8') as jsonfile:
            json.dump(data_j,jsonfile,indent=2,ensure_ascii=False)
    jsonfile.close()
    return data_j 


# Run Example
#DataFile=".\Grafana\TA\Data.xlsx"
#JsonFile=".\Grafana\TA\Grafana.json"
#BJson=get_B_drawio_xml_string(JsonFile)
#correct_Gr_Json(JsonFile,DataFile,BJson)
