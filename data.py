import pandas as pd

COLUMNS = {
    "product_sku": ['product_code', 'brand_series', 'category_ids', 'product_release_end_date', 'color_number', 'product_sku_main_image', 'all_ingredients', 'color_impression', 'how_to_use', 'product_sku_sub_image',
                    'product_release_start_date', 'pa', 'spf', 'capacity', 'color_name', 'movie', 'how_to_use', 'color_ball_image', 'how_to_set', 'limit_flg', 'price_include_tax_10', 'search_keyword', 'price_exclude_tax', 'deleted_at'],
    "product_group": ['product_codes', 'product_group_cd', 'product_group_name_jp', 'url', 'product_main_catch', 'product_release_start_date', 'product_release_end_date', 'representative_product_code',
                      'set_price_include_tax_10', 'type_name', 'product_catch_comment', 'limited_flg', 'product_description_text', 'product_description_note3', 'product_description_note2', 'product_description_note1', 'product_sub_catch', 'deleted_at'],
    "category": ['category_id', 'category_name_jp'],
    "product_relation": ["relation_product_group_cd", "product_group_cd"],
    "best_cosmetics_history": ["product_codes", "award_name", "department_name", "magazine"]
}

FILES_NEEDED = [
    {
        "name": "category",
        "label": "Category",
        "filename": "category.csv",
    },
    {
        "name": "product_group",
        "label": "Product Group",
        "filename": "product_group.csv",
    },
    {
        "name": "product_sku",
        "label": "Product SKU",
        "filename": "product_sku.csv",
    },
    {
        "name": "product_relation",
        "label": "Product Relation",
        "filename": "product_relation.csv",
    },
    {
        "name": "best_cosmetics_history",
        "label": "Best Cosmetics History",
        "filename": "best_cosmetics_history.csv",
    }
]

FILE_PATHS = {
    "product_sku": "",
    "product_group": "",
    "category": "",
    "product_relation": "",
    "best_cosmetics_history": "",
}

DATAFRAMES = {
    "product_sku": pd.DataFrame(),
    "product_group": pd.DataFrame(),
    "category": pd.DataFrame(),
    "product_relation": pd.DataFrame(),
    "best_cosmetics_history": pd.DataFrame(),
}
