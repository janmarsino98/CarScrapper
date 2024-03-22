import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import time
from requests_ip_rotator import ApiGateway, EXTRA_REGIONS

def main():
    
    scrap_remaining_engines()
    
    
    print("Done...")
    
def scrap_remaining_engines():
    session_scraps = 0
    df = pd.read_excel("engines.xlsx", index_col=0)
    for index, row in df.iterrows():
        if pd.isna(row["standard_power"]):
            session_scraps += 1
            print(f"Scrapping row {index} with the link {row['link']}...")
            result = scrap_car_info(row["link"])
            if result:
                standard_power, nano_power, standard_torque, nano_torque, engine_ecu = result
                df.at[index, "standard_power"] = standard_power
                df.at[index, "nano_power"] = nano_power
                df.at[index, "standard_torque"] = standard_torque
                df.at[index, "nano_torque"] = nano_torque
                df.at[index, "engine_ecu"] = engine_ecu
            
            else:
                df.at[index, "standard_power"] = "None"
                df.at[index, "nano_power"] = "None"
                df.at[index, "standard_torque"] = "None"
                df.at[index, "nano_torque"] = "None"
                df.at[index, "engine_ecu"] = "None"
                
            if index % 50 == 0:
                df.to_excel("engines.xlsx")
            
            if session_scraps == 400:
                time.sleep(300)
                session_scraps = 0
            
            
def scrap_car_info(link):
    gateway = ApiGateway(site="https://www.transfermarkt.es")
    gateway.start()
    session = requests.Session()
    response = requests.get(link)
    s = bs(response.text, "html.parser")
    first_h2 = s.find("h2").text
    print(first_h2)
    if first_h2 == "At this moment the tuning file is not available yet, we are still working on this.":
        return None
    
    else:
        box1, box2 = s.find_all("div", class_="chiptuning-files-results-graph-data")
        standard_power_div, nano_power_div = box1.find_all("div", class_="chiptuning-files-results-graph-row")[0:2]
        standard_power = standard_power_div.find_all("div", class_="chiptuning-files-results-graph-col")[1].text.strip()
        nano_power = nano_power_div.find_all("div", class_="chiptuning-files-results-graph-col")[1].text.strip()
        
        standard_torque_div, nano_torque_div = box2.find_all("div", class_="chiptuning-files-results-graph-row")[0:2]
        standard_torque = standard_torque_div.find_all("div", class_="chiptuning-files-results-graph-col")[1].text.strip()
        nano_torque = nano_torque_div.find_all("div", class_="chiptuning-files-results-graph-col")[1].text.strip()
        
        enginge_table = s.find("div", class_="faq-item_content chiptuning-files-results-specs")
        engine_ecu = enginge_table.find_all("table")[-1].find_all("td")[-1].text.strip()
        
        return standard_power, nano_power, standard_torque, nano_torque, engine_ecu

def get_all_engines():
    all_engines = []
    df = pd.read_excel("generations.xlsx", index_col=0)
    total_rows = df["link"].count()
    for index, row in df.iterrows():
        print(f"Scrapping engines from model {index + 1} out of {total_rows}")
        engines = get_brand_models(row["link"])
        all_engines.extend(engines)
        
        if index % 50 == 0:
            df = pd.DataFrame(all_engines, columns = ["link"])
            df.to_excel("engines.xlsx")
            print("Information was stored...")
    df = pd.DataFrame(all_engines, columns = ["link"])
    df.to_excel("engines.xlsx")
    

def get_all_generations():
    all_generations = []
    df = pd.read_excel("models.xlsx", index_col=0)
    total_rows = df["link"].count()
    for index, row in df.iterrows():
        print(f"Scrapping generations from model {index + 1} out of {total_rows}")
        generations = get_brand_models(row["link"])
        all_generations.extend(generations)
        
    df = pd.DataFrame(all_generations, columns = ["link"])
    df.to_excel("generations.xlsx")


def get_all_models():
    all_models = []
    df = pd.read_excel("brands.xlsx", index_col=0)
    for index, row in df.iterrows():
        print(f"Scrapping brand {index+1}")
        brand_models = get_brand_models(row["link"])
        all_models.extend(brand_models)
        
    df = pd.DataFrame(all_models, columns=["link"])
    df.to_excel("models.xlsx")
        

def get_brand_models(brand_link):
    model_links = []
    response = requests.get(brand_link)
    s = bs(response.text, "html.parser")
    container = s.find("div", class_="chiptuning-files-models-overview chiptuning-files-models-overview--alt reveal")
    models = container.find_all("a")
    for model in models:
        model_links.append(model["href"])
    return model_links
    
def get_brand_links_and_create_df():
    mark_links = []
    response = requests.get("https://www.dyno-chiptuningfiles.com/tuning-file/")
    s = bs(response.text, "html.parser")
    brands = s.find_all("div", class_="chiptuning-files-brands-overview-item")
    for brand in brands:
        link = brand.find("a")["href"]
        mark_links.append(link)
        print("Link added...")
    
    df = pd.DataFrame(mark_links, columns = ["link"])
    df.to_excel("brands.xlsx")
    

    
if __name__ == "__main__":
    main()