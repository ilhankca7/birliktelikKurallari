from datetime import datetime, timedelta
from association import Association
from basket import Basket

directory = "../../data1/data"
prefix = "igcdgz"

def get_last_12_months():
    end_date = datetime.now()
    months = []

    for i in range(15):
        year = end_date.year
        month = end_date.month
        months.append((year, month))
        if month == 1:
            end_date = end_date.replace(year=year - 1, month=12)
        else:
            end_date = end_date.replace(month=month - 1)
    months.reverse()
    return months

def runs():
    last_12_months = get_last_12_months()
    for year, month in last_12_months:
        print(f"Yıl: {year}, Ay: {month}")
        setBasket(year,month)


def setBasket(year, month):

    #year, month = 2023, 9  # Örnek ayar; istediğiniz tarihleri buradan seçebilirsiniz.
    association = Association(directory, year, month, prefix)
    association.init()

    if association.association is not None and not association.association.empty:
        basket = Basket(association.association)
        df = association.basket
        result_df = basket.process(df, association.association)
        return result_df
    else:
        print("Kural seti oluşturulamadı.")
        return None

if __name__ == "__main__":
    runs()

