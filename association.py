import json
import pandas as pd
import os
from pymongo import errors
from fpgrowth_py import fpgrowth
from mongo import Mongo


class Association:
    def __init__(self, directory, year, month, prefix):
        self.directory = directory
        self.year = year
        self.month = month
        self.prefix = prefix
        self.df = None
        self.basket = None
        self.association = None

    def fileload(self, path):
        try:
            with open(path, "r", encoding="utf-8") as file:
                data = json.load(file)
                print(f"Dosya yüklendi: {path}, içerik tipi: {type(data)}")
                return data
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Dosya yükleme hatası: {e}")
            return []

    def getFileList(self):
        fileList = [f"{self.prefix}_orderdetail_{self.year}_{self.month}.json"]
        return fileList

    def getData(self):
        data = []
        print("Dizin içindeki dosyalar:")
        for filename in self.getFileList():
            path = os.path.join(self.directory, filename)
            if os.path.isfile(path):
                temp = self.fileload(path)
                if len(temp) > 0:
                    data.extend(temp)
                else:
                    print(f"Dosya '{path}' boş.")
            else:
                print(f"Dosya '{path}' mevcut değil.")
        return data

    def setDataFrame(self):
        data = self.getData()
        if len(data) > 0:
            self.df = pd.DataFrame(data)
            print(f"Veri çerçevesi oluşturuldu, toplam {len(self.df)} satır.")
        else:
            print("Veri çerçevesi oluşturulamadı; veri yok.")

    def setBasketGroup(self):
        if self.df is not None:
            self.basket = self.df.groupby(["OrderCode", "CustomerCode"]).agg(
                {"ProductCode": lambda s: list(set(s))}).reset_index()
            print(f"Sepet grubu oluşturuldu, toplam {len(self.basket)} grup.")
        else:
            print("Veri çerçevesi yüklenmedi; sepet grubu oluşturulamadı.")

    def setRules(self):
        if self.basket is not None:
            print("FPGrowth algoritması çalıştırılıyor...")
            try:
                # Destek ve güven oranlarını daha düşük ayarlayarak kural oluşturma şansını artırıyoruz.
                freqItemset, rules = fpgrowth(
                    self.basket["ProductCode"].values, minSupRatio=0.005, minConf=0.005)
                print(f"Kurallar oluşturuldu, toplam {len(rules)} kural.")
                return freqItemset, rules
            except Exception as e:
                print(f"FPGrowth hatası: {e}")
                return None, None
        else:
            print("Sepet verisi yüklenemedi.")
            return None, None

    def setTable(self):
        freqItemset, rules = self.setRules()
        if rules is not None and len(rules) > 0:
            self.association = pd.DataFrame(
                rules, columns=["Basket", "Next_Product", "Proba"])
            self.association = self.association.sort_values(
                by="Proba", ascending=False)
            self.association["Basket"] = self.association["Basket"].apply(
                lambda x: list(x))
            self.association["Next_Product"] = self.association["Next_Product"].apply(
                lambda x: list(x))
            print("Kural tablosu başarıyla oluşturuldu.")
        else:
            print("Kurallar oluşturulamadı veya kural yok.")

    def update_basket_data(self, data):
        db = Mongo()
        try:
            collection = "association"
            for item in data:
                basket = item.get("Basket")
                if basket:
                    basket_id = list(basket)
                    where = {"Basket": basket}
                    update = {
                        "$set": {"Basket": basket},
                        '$push': {'Proba': item["Proba"]},
                        "$addToSet": {"Next_Product": {"$each": list(item["Next_Product"])}}
                    }

                    result = db.updateOne(
                        collection, where, update, upsert=True)
                    if result.matched_count:
                        print(f"Sepet {basket_id} başarıyla güncellendi.")
                    elif result.upserted_id:
                        print(f"Yeni sepet eklendi: {basket_id}")
        except errors.PyMongoError as e:
            print(f"MongoDB hatası: {e}")
        finally:
            db.mongoClose()

    def init(self):
        self.setDataFrame()
        self.setBasketGroup()
        self.setTable()
        if self.association is not None and not self.association.empty:
            data = self.association.to_dict(orient="records")
            self.update_basket_data(data)
        else:
            print("Association tablosu oluşturulamadı; işlem tamamlanamadı.")
