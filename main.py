import pandas as pd
import json

def init():
    SPENDING_COLUMN_NAME = 'Spending amount'
    BANK_CURRENCY = 'UAH'
    MAIN_CURRENCY = 'USD'
    data_init('en', '11_gen.csv')
    data_cleaning()
    conversion_rates_init(BANK_CURRENCY)
    unite_all_currencies(MAIN_CURRENCY, BANK_CURRENCY, SPENDING_COLUMN_NAME)
    categorisation([True, True, True, False], SPENDING_COLUMN_NAME)
    cashback_calculation(MAIN_CURRENCY)
    commission_calculation(MAIN_CURRENCY)
    manage_columns([], SPENDING_COLUMN_NAME)
    print(commission_calculation(MAIN_CURRENCY))


def data_init(LANG: str, DATA_FILE_NAME: str) -> None:
    global data_set, mcc_data
    DATA_FILE = f"data/bills/{DATA_FILE_NAME}"
    MCC_FILE = f'data/mcc/mcc-{LANG}.json'

    try:
        data_set = pd.read_csv(DATA_FILE)
    except FileNotFoundError:
        raise FileNotFoundError(f'File {DATA_FILE} does not exist or incorrect name/path')
    except pd.errors.EmptyDataError:
        raise ValueError('Your database is empty')
    
    try:
        with open(MCC_FILE, 'r') as file_json:
            mcc_data_json = json.load(file_json)  # This will load it as a list of dictionaries
        mcc_data = pd.DataFrame(mcc_data_json)
    except FileNotFoundError:
        raise FileNotFoundError(f'File {MCC_FILE} does not exist or incorrect name/path')
    except json.JSONDecodeError:
        raise KeyError(f'File {MCC_FILE} cannot be decoded')
    

def ensuring_data_type(NOT_INT_INDEXES: list[int]) -> None:
    for i in range(data_set.shape[1]):
        if i not in NOT_INT_INDEXES:
            data_set.iloc[:, i] = pd.to_numeric(data_set.iloc[:, i], errors='coerce')

    
def data_cleaning():
    dates_cleaning(0)
    ensuring_data_type([0, 1, 2, 5])

def manage_columns(dropped_columns: list[int], SPENDING_COLUMN_NAME) -> object:
    data_set.drop(data_set.columns[dropped_columns], axis=1, inplace=True)
    spending_column = data_set.pop(SPENDING_COLUMN_NAME)
    data_set.insert(2, SPENDING_COLUMN_NAME,spending_column)

    return data_set


def dates_cleaning(col_number: int ) -> object:
    # It is to remove precise timing of the transaction (date only) -> easy to read for user
    data_col = data_set.iloc[:, col_number]
    data_set.iloc[:, col_number] = data_col.str.split(' ').str[0]

    return data_set
    

def categorisation(data_display: list[bool], SPENDING_COLUMN_NAME) -> object:
    MCC_COLUMN_NAME = data_set.columns[2]
    AMOUNT_COLUMN_NAME = SPENDING_COLUMN_NAME

    agg_data = data_set.groupby(MCC_COLUMN_NAME)[AMOUNT_COLUMN_NAME].sum().reset_index()

    # Convert to integer
    agg_data[MCC_COLUMN_NAME] = agg_data[MCC_COLUMN_NAME].astype(int)
    mcc_data['mcc'] = mcc_data['mcc'].astype(int)

    # Rename 'mcc' in mcc_data to match MCC_COLUMN_NAME
    mcc_data.rename(columns={'mcc': MCC_COLUMN_NAME}, inplace=True)

    agg_data = pd.merge(
        agg_data,  # Left DataFrame 
        mcc_data,  # Right DataFrame
        how='left',  # Preserve all rows from aggregated_data
    )

    if len(data_display) != len(agg_data.columns):
        print('Error, array not possible')
    for i, item in enumerate(data_display):
        if not item:
            agg_data.drop(agg_data.columns[i], axis=1, inplace=True)

    return agg_data

def conversion_rates_init(BANK_CURRENCY) -> dict:
    currencies_set = data_set.iloc[:, 5].unique().tolist()
    global conversion_rates
    conversion_rates = {}
    for currency in currencies_set:
        if currency == BANK_CURRENCY:
            conversion_rates[currency] = 1
        else:
            rows_with_currency = data_set[data_set.iloc[:, 5] == currency]
            conversion_rates[currency] = rows_with_currency.iloc[:, 6].mean()

    return conversion_rates

def unite_all_currencies(MAIN_CURRENCY: str, BANK_CURRENCY: str, SPENDING_COLUMN_NAME: str = 'Spending amount') -> object:
    if BANK_CURRENCY == MAIN_CURRENCY:
        data_set.loc[:, SPENDING_COLUMN_NAME] = data_set.iloc[:, 3]
        return data_set
    
    currencies_set = data_set.iloc[:, 5].unique().tolist()


    if MAIN_CURRENCY not in currencies_set or BANK_CURRENCY not in currencies_set:
        raise ValueError('Currency not found in the dataset')
    
    currencies_set.remove(MAIN_CURRENCY)
    currencies_set.remove(BANK_CURRENCY)
    currencies_set.insert(0, BANK_CURRENCY)
    currencies_set.insert(1, MAIN_CURRENCY)

    for currency in currencies_set:
        currency_rows = data_set[data_set.iloc[:, 5] == currency]

        data_set.loc[currency_rows.index, SPENDING_COLUMN_NAME] = (
            currency_rows.iloc[:, 3] / conversion_rates[MAIN_CURRENCY]
        )
    
    data_set.loc[:, SPENDING_COLUMN_NAME] = data_set.loc[:, SPENDING_COLUMN_NAME].round(2)
    data_set.iloc[:, 5] = MAIN_CURRENCY

    return data_set

def cashback_calculation(MAIN_CURRENCY):
    cashback_sum = data_set.iloc[:, 8].sum()
    cashback_sum = (cashback_sum / conversion_rates[MAIN_CURRENCY]).round(2)
    return f'{cashback_sum} {MAIN_CURRENCY}'

def commission_calculation(MAIN_CURRENCY):
    commission_sum = data_set.iloc[:, 8].sum()
    commission_sum = (commission_sum / conversion_rates[MAIN_CURRENCY]).round(2)
    return f'{commission_sum} {MAIN_CURRENCY}'
        
# initialisation
init()