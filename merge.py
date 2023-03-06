from datetime import date
import pandas as pd
from data import COLUMNS, DATAFRAMES, FILE_PATHS
import json


def flatten_list(nested_list):
    flat_list = []
    for sublist in nested_list:
        for item in sublist:
            if item not in flat_list:
                flat_list.append(item)
    return flat_list


def get_category_name(category_ids):
    ids = category_ids.split(',')
    ids = [id for id in ids if id != '']
    output = []
    for id in ids:
        output.append(DATAFRAMES['category'].loc[lambda x: x['category_id']
                                                 == int(id), 'category_name_jp'].values[0])
    return ",".join(output)


def get_image_position(product_code, df):
    rep_col_product = df[df
                         ['product_code'] == product_code]['representative_product_code'].values[0]
    if rep_col_product == product_code:
        return 1
    else:
        products = df[df
                      ['product_code'] == product_code]['product_codes'].values[0].split(',')
        products = [x for x in products if x != rep_col_product]
        products.sort(reverse=True)
        return products.index(product_code) + 2


def replace_path(cell):
    return str(cell).replace('public/', 'https://api.albion.co.jp/storage/') if 'public/' in str(cell) else cell


def get_price_array(row):
    output = [str(row['price_include_tax_10'])]
    for price in row['set_price_include_tax_10'].split(','):
        if price and price != '0':
            output.append(price)
    output = list(dict.fromkeys(output))
    return json.dumps(output, ensure_ascii=False)


def create_image_array(sub_image):
    json_data = json.loads(sub_image)
    if len(json_data) == 0:
        return
    else:
        output = list(replace_path(i['image'])
                      for i in json_data.get('product_sku_sub_image'))
        output = list(i.replace('"', '') for i in output)
        return json.dumps(output, ensure_ascii=False)


def create_award_magazine_array(product_code):
    results = DATAFRAMES['best_cosmetics_history'].loc[lambda x: x['product_codes'].str.contains(
        product_code, na=False)]
    if results.empty:
        return
    else:
        output = flatten_list(results['magazine'].values[0])
        return json.dumps(output, ensure_ascii=False)


def create_award_department_name(product_code):
    results = DATAFRAMES['best_cosmetics_history'].loc[lambda x: x['product_codes'].str.contains(
        product_code, na=False)]
    if results.empty:
        return
    else:
        output = flatten_list(results['department_name'].values[0])
        return json.dumps(output, ensure_ascii=False)


def create_award_name(product_code):
    results = DATAFRAMES['best_cosmetics_history'].loc[lambda x: x['product_codes'].str.contains(
        product_code, na=False)]
    if results.empty:
        return
    else:
        output = flatten_list(results['award_name'].values[0])
        return json.dumps(output, ensure_ascii=False)


def create_modal_btn_text(row):
    output = []
    if row['all_ingredients']:
        output.append('成分表示')
    if row['how_to_use']:
        output.append('使用方法')
    if row['how_to_set']:
        output.append('セット方法')
    return str(output).replace("'", '"')


def mergeFiles():
    try:
        DATAFRAMES['product_sku'] = pd.read_csv(
            FILE_PATHS['product_sku'], usecols=COLUMNS['product_sku'])
        DATAFRAMES['product_group'] = pd.read_csv(
            FILE_PATHS['product_group'], usecols=COLUMNS['product_group'],
            dtype={'url': 'string'},
            keep_default_na=False,
        ).assign(url=lambda x: x['url'].str.rstrip('/').str.split('/').str[-1])
        DATAFRAMES['category'] = pd.read_csv(
            FILE_PATHS['category'], usecols=COLUMNS['category'])
        DATAFRAMES['product_relation'] = pd.read_csv(
            FILE_PATHS['product_relation'], usecols=COLUMNS['product_relation'])
        DATAFRAMES['best_cosmetics_history'] = pd.read_csv(
            FILE_PATHS['best_cosmetics_history'], usecols=COLUMNS['best_cosmetics_history']).apply(lambda x: x.str.split(','), axis=1).explode('product_codes').groupby('product_codes', as_index=False).agg(list)

        DATAFRAMES['product_group']['product_release_start_date'] = pd.to_datetime(
            DATAFRAMES['product_group']['product_release_start_date'], errors='coerce', format='%Y-%m-%d')
        DATAFRAMES['product_group']['product_release_end_date'] = pd.to_datetime(
            DATAFRAMES['product_group']['product_release_end_date'], errors='coerce', format='%Y-%m-%d')
        DATAFRAMES['product_group']['deleted_at'] = pd.to_datetime(
            DATAFRAMES['product_group']['deleted_at'], errors='coerce', format='%Y-%m-%d')
        DATAFRAMES['product_sku']['product_release_end_date'] = pd.to_datetime(
            DATAFRAMES['product_sku']['product_release_end_date'], errors='coerce', format='%Y-%m-%d')
        DATAFRAMES['product_sku']['product_release_start_date'] = pd.to_datetime(
            DATAFRAMES['product_sku']['product_release_start_date'], errors='coerce', format='%Y-%m-%d')
        DATAFRAMES['product_sku']['deleted_at'] = pd.to_datetime(
            DATAFRAMES['product_sku']['deleted_at'], errors='coerce', format='%Y-%m-%d')

        df1 = DATAFRAMES['product_sku']
        df2 = DATAFRAMES['product_group']
        df1['join'] = 1
        df2['join'] = 1
        dfm = df1.merge(df2, on='join', how='outer',
                        suffixes=('_sku', '_group')).drop('join', axis=1)
        df2.drop('join', axis=1, inplace=True)
        dfm['match'] = dfm.apply(lambda x: x['product_code']
                                 in x['product_codes'].split(','), axis=1)
        df = dfm[dfm['match']].drop('match', axis=1)

        df['product_main_catch'] = df['product_main_catch'].str.replace(
            r'<[^>]*>', '', regex=True)

        df_output = pd.DataFrame({
            "Handle": df['url'],
            "Vendor": df['brand_series'],
            "Command": "MERGE",
            "Title": df['product_group_name_jp'],
            "Body HTML": df['product_main_catch'],
            "category_name_en": df['category_ids'].apply(lambda x: get_category_name(x)),
            "Tags Command": 'REPLACE',
            "Status": 'Active',
            "Published": df['product_release_end_date_group'].apply(
                lambda x: 'FALSE' if x < pd.Timestamp(date.today()) else 'TRUE'),
            "Published Scope": "web",
            "Gift Card": 'FALSE',
            "Row #": range(1, len(df) + 1),
            "Category: ID": 2692,
            "Smart Collections": df['category_ids'].apply(lambda x: get_category_name(x)),
            "Variant Command": df.apply(
                lambda x: 'DELETE' if x['product_release_end_date_sku'] < pd.Timestamp(date.today())
                # lambda x: '' if x['product_release_end_date_sku'] < pd.Timestamp(date.today())
                or x['deleted_at_sku'] > pd.Timestamp(date.today())
                or x['deleted_at_group'] > pd.Timestamp(date.today())
                else 'MERGE', axis=1),
            "Option1 Name": '色',
            "Option1 Value": df['color_number'],
            "Variant Position": df['product_code'].apply(lambda x: get_image_position(x, df)),
            "Variant SKU": df['product_code'],
            "Variant Image": df['product_sku_main_image'].apply(lambda x: replace_path(x)),
            "Variant Metafield: custom.variant_main_image [url]": df['product_sku_main_image'].apply(lambda x: replace_path(x)),
            "Variant Price": df['price_exclude_tax'],
            "Variant Taxable": 'TRUE',
            "Variant Inventory Policy": 'continue',
            "Variant Fulfillment Service": 'manual',
            "Variant Requires Shipping": 'FALSE',
            "Image Position": df['product_code'].apply(lambda x: get_image_position(x, df)),
            "Inventory Available: 京橋1-12-2": "Stocked",
            "Metafield: title_tag [string]": df['product_group_name_jp'],
            "Metafield: custom.price [list.single_line_text_field]": df.apply(lambda x: get_price_array(x), axis=1),
            "Variant Metafield: custom.product_code [single_line_text_field]": df['product_code'],
            "Variant Metafield: custom.all_ingredients [single_line_text_field]": df['all_ingredients'],
            "Variant Metafield: custom.color_number [single_line_text_field]": df['color_number'],
            "Variant Metafield: custom.color_impression [single_line_text_field]": df['color_impression'],
            "Variant Metafield: custom.search_keyword [single_line_text_field]": df['search_keyword'],
            "Variant Metafield: custom.how_to_use [json]": df['how_to_use'].apply(lambda x: replace_path(x)),
            "Variant Metafield: custom.images [list.url]": df['product_sku_sub_image'].apply(lambda x: create_image_array(x)),
            "Variant Metafield: custom.product_sku_release_start_date [date]": df['product_release_start_date_sku'],
            "Variant Metafield: custom.pa_value [single_line_text_field]": df['pa'],
            "Variant Metafield: custom.spf [single_line_text_field]": df['spf'],
            "Variant Metafield: custom.capacity [single_line_text_field]": df['capacity'],
            "Variant Metafield: custom.color_name [single_line_t/ext_field]": df['color_name'].apply(lambda x: str(x).split('||') if '||' in str(x) else x),
            "Variant Metafield: custom.movie [json]": df['movie'].apply(lambda x: x if x != "[]" else ''),
            "Variant Metafield: custom.product_sku_release_end_date [date]": df['product_release_end_date_sku'].apply(lambda x: x.strftime('%Y-%m-%d') if x > pd.Timestamp(date.today()) else ''),
            "Variant Metafield: custom.color_ball_image [url]": df['color_ball_image'].apply(
                lambda x: replace_path(x)),
            "Metafield: custom.type_name [list.single_line_text_field]": df['type_name'],
            "Metafield: custom.product_group_cd [single_line_text_field]": df['product_group_cd'],
            "Metafield: custom.award_magazine [list.single_line_text_field]": df['product_code'].apply(
                lambda x: create_award_magazine_array(x)),
            "Metafield: custom.product_catch_comment [single_line_text_field]": df['product_catch_comment'],
            "Metafield: custom.product_description_text [multi_line_text_field]": df['product_description_text'],
            "Metafield: custom.product_description_note3 [multi_line_text_field]": df['product_description_note3'],
            "Metafield: custom.product_description_note2 [multi_line_text_field]": df['product_description_note2'],
            "Metafield: custom.product_description_note1 [multi_line_text_field]": df['product_description_note1'],
            "Metafield: custom.product_sub_catch [multi_line_text_field]": df['product_sub_catch'],
            "Metafield: custom.award_department_name [list.single_line_text_field]": df['product_code'].apply(
                lambda x: create_award_department_name(x)),
            "Metafield: custom.award_name [list.single_line_text_field]": df['product_code'].apply(
                lambda x: create_award_name(x)),
            "Metafield: custom.modal_btn_text [list.single_line_text_field]": df.apply(
                lambda x: create_modal_btn_text(x), axis=1),
            "Metafield: custom.relation_product_group_cd [list.single_line_text_field]": df["product_group_cd"].apply(
                lambda x: DATAFRAMES['product_relation'][DATAFRAMES['product_relation']['product_group_cd'] == x]['relation_product_group_cd'].values).apply(
                lambda x: str(x).replace("'", '"') if len(x) > 0 else ''),
            "Metafield: custom.product_group_release_start_date [date]": df['product_release_start_date_group'],
            "Metafield: custom.product_group_release_end_date [date]": df['product_release_end_date_group'].apply(
                lambda x: x.strftime('%Y-%m-%d') if x > pd.Timestamp(date.today()) else ''),
            "Variant Metafield: custom.how_to_set [json]": df['how_to_set'].apply(lambda x: x if x != "[]" else '').apply(lambda x: replace_path(x)),
            "Variant Metafield: custom.limit_sku_flg [number_integer]": df['limit_flg'],
            # "Variant Metafield: custom.limit_sku_flg [number_integer]": df['limit_flg'].apply(lambda x: "" if x == 1 else x),
            "Metafield: custom.limit_group_flg [number_integer]": df['limited_flg']
            # "Metafield: custom.limit_group_flg [number_integer]": df['limited_flg'].apply(lambda x: "" if x == 1 else x)
        })

        return {"error": "", "data": df_output}

    except Exception as e:
        print(e)
        return {"error": e, "data": ""}
