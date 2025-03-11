import pandas as pd

class Basket:
    def __init__(self, association_data):
        self.association = association_data

    def compute_next_best_product(self, basket_el):
        basket_el = list(basket_el)
        for k in basket_el:
            k = {k}
            matching_rows = self.association[self.association["Basket"].apply(lambda x: set(x) == k)]
            if not matching_rows.empty:
                next_pdt = list(matching_rows["Next_Product"].values[0])[0]
                if next_pdt not in basket_el:
                    proba = matching_rows["Proba"].values[0]
                    return next_pdt, proba
        return 0, 0

    def find_next_product(self, basket):
        list_next_pdt = []
        list_proba = []
        for product in basket["ProductCode"]:
            el = set(product)
            matching_rows = self.association[self.association["Basket"].apply(lambda x: set(x) == el)]
            if not matching_rows.empty:
                next_pdt = list(matching_rows["Next_Product"].values[0])[0]
                proba = matching_rows["Proba"].values[0]
            else:
                next_pdt, proba = self.compute_next_best_product(product)
            list_next_pdt.append(next_pdt)
            list_proba.append(proba)
        return list_next_pdt, list_proba

    def process(self, df, association):
        if association is None or association.empty:
            print("Association tablosu boş; işlem gerçekleştirilemiyor.")
            return df
        self.association = association
        next_products, probabilities = self.find_next_product(df)
        df["Next_Product"] = next_products
        df["Probability"] = probabilities
        return df
