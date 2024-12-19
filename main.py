import pandas as pd
import json

def init():
    data_init('en', '11_gen.csv')
    data_cleaning()
    ensuring_data_type([0, 1, 2, 5])
    unite_all_currencies('GBP')
    categorisation([True, True, True, False])


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

def dates_cleaning(col_number: int ) -> object:
    # It is to remove precise timing of the transaction (date only) -> easy to read for user
    data_col = data_set.iloc[:, col_number]
    data_set.iloc[:, col_number] = data_col.str.split(' ').str[0]

    return data_set
    

def categorisation(data_display: list[bool]) -> object:
    MCC_COLUMN_NAME = data_set.columns[2]
    AMOUNT_COLUMN_NAME = data_set.columns[4]

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

    print(agg_data)


def unite_all_currencies(main_currency):
    currencies_set = data_set.iloc[:, 5].unique()

    if main_currency not in currencies_set:
        print(f"Error: {main_currency} not found in the dataset.")
        return

    EXCHANGE_RATE_MEAN = data_set.loc[data_set.iloc[:, 5] == main_currency, data_set.columns[6]].mean()
    
    for item in currencies_set:
        if item == main_currency:
            continue
        conversion_rate = data_set.loc[data_set.iloc[:, 5] == item, data_set.columns[6]].mean()
        data_set.loc[data_set.iloc[:, 5] == item, data_set.columns[4]] *= conversion_rate / EXCHANGE_RATE_MEAN

    data_set.iloc[:, 5] = main_currency

# initialisation
init()