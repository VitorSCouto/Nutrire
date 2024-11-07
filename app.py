from flask import Flask, jsonify, request
import pandas as pd
import glob
import os
import requests
import concurrent.futures

app = Flask(__name__)

wsgi_app = app.wsgi_app

def carregareprocessararquivoscsv(folder_path):
    csvs = glob.glob(os.path.join(folder_path, "*.csv"))
    
    data_frames = []
    
    for csv in csvs:
        try:
            df = pd.read_csv(csv, delimiter=';', on_bad_lines='skip')
            data_frames.append(df)
        except pd.errors.ParserError:
            print(f"Erro ao ler csv")
    
    dados = pd.concat(data_frames, ignore_index=True, sort=False)
    dados = adicionarCidade(dados)
    dados = adicionarSegmento(dados)
    dados = adicionarCanal(dados)

    return dados

def categorizarSegmento(cnae):

    cnaeParaSegmento = {
        "4789004": "PET SHOP",
        "9609208": "PET SHOP",
        "4623109": "AGROPECUARIA",
        "4771704": "VETERINARIA",
        "7500100": "VETERINARIA",
        "4644302": "VETERINARIA",
        "0159802": "CRIADOR",
        "8011102": "ADESTRADOR",
        "9609207": "HOTEL PET",
        "4712100": "MINIMERCADO",
        "4711302": "HIPERMERCADO",
        "4691500": "PARCEIRO",
        "4639702": "PARCEIRO"
    }
    cnae_str = str(cnae)

    return cnaeParaSegmento.get(cnae_str, "OUTRO")

def adicionarSegmento(df):
    df['segmento'] = df['cnae_principal'].apply(categorizarSegmento)
    return df

def canalSegmento(canal):

    segmentoParaCanal = {
        "PET SHOP": "ESPECIALIZADO",
        "AGROPECUARIA": "ESPECIALIZADO",
        "VETERINARIA": "ESPECIALIZADO",
        "CRIADOR": "ESPECIALIZADO",
        "ADESTRADOR": "ESPECIALIZADO",
        "HOTEL PET": "ESPECIALIZADO",
        "MINIMERCADO": "AUTOSERVICO",
        "SUPERMERCADO": "AUTOSERVICO",
        "HIPERMERCADO": "AUTOSERVICO",
        "PARCEIRO": "DISTRIBUIDOR",
    }
    canal_str = str(canal)

    return segmentoParaCanal.get(canal_str, "-")

def adicionarCanal(df):
    df['canal'] = df['segmento'].apply(canalSegmento)
    return df

def salvar_em_excel(data_frame, file_name):
    try:
        data_frame.to_excel(file_name, index=False) 
        print(f"Data successfully saved to {file_name}")
    except Exception as e:
        print(f"Error saving to Excel: {e}")

city_cache = {}

def fetchCidade(municipio_id):
    print(municipio_id)
    if municipio_id in city_cache:
        return city_cache[municipio_id]
    
    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/municipios/{municipio_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Will raise an exception for 4xx and 5xx status codes
        
        data = response.json()
        city_name = data.get('nome', None)
        
        # Cache the result for future use
        city_cache[municipio_id] = city_name
        return city_name
    except requests.RequestException as e:
        print(f"Error fetching data for municipio-id {municipio_id}: {e}")
        return None

def adicionarCidade(df):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        df['cidade'] = list(executor.map(fetchCidade, df['municipio-id']))
    
    return df



def empresas_na_regiao(df):
    filtered = df[(df['canal'] == 'ESPECIALIZADO')]
    resultado = filtered.groupby(['canal', 'cidade']).size().reset_index(name='total')
    return resultado

def cidade_com_mais_petshop(df):

    filtered = df[df['segmento'] == 'PET SHOP']

    resultado = filtered.groupby(['cidade']).size().reset_index(name='total').sort_values(by='total', ascending=False).head(1)

    cidade_top = resultado['cidade'].iloc[0]

    cidade_qtd = int(resultado['total'].iloc[0])

    filtered_cidade = filtered[filtered['cidade'] == cidade_top]
    bairro_data = filtered_cidade.groupby(['bairro']).size().reset_index(name='total').sort_values(by='total', ascending=False)
    resposta = {
        "Cidade": cidade_top,
        "total": cidade_qtd,
        "segmento": "PET SHOP",
        "bairros": bairro_data.to_dict(orient='records')
    }

    return resposta 


def hipermercados_na_regiao(df):
    filtered = df[df['segmento'] == 'HIPERMERCADO']
    
    resultado = filtered.groupby(['segmento', 'cidade']).size().reset_index(name='total')
    
    resultado = resultado[resultado['total'] > 0]
    
    return resultado[['segmento', 'cidade', 'total']]


@app.route('/', methods=['GET'])
def main():
    arquivos = carregareprocessararquivoscsv("dataset_empresa")

    pergunta = request.args.get('pergunta', default='0', type=str)
    
    if pergunta == '1': 
        resultado = empresas_na_regiao(arquivos)
        pergunta_respondida = "Empresas especializadas na regiao"
    elif pergunta == '2': 
        resultado = cidade_com_mais_petshop(arquivos)
        pergunta_respondida = "Cidade e seus bairros com mais petshops"
    elif pergunta == '3':
        resultado = hipermercados_na_regiao(arquivos)
        pergunta_respondida = "Cidades com hipermercados"
    elif pergunta == '4':
        salvar_em_excel(arquivos, "analise_empresas.xlsx")
        pergunta_respondida = "Salvar Arquivo"
        resultado = "Nome: analise_empresas.xlsx"
    else:
        return jsonify({"error": "Pergunta nao reconhecida"})

    if pergunta == '2' or pergunta == '4':
       return jsonify({
         "pergunta": pergunta_respondida,
         "resposta": resultado
        })
    else:
        return jsonify({
            "pergunta": pergunta_respondida,
            "resposta": resultado.to_dict(orient='records')
        })

if __name__ == '__main__':
    import os
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = 8080
    except ValueError:
        PORT = 5555
    app.run(HOST, PORT)
