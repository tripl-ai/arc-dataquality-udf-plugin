SELECT
    is_valid_phonenumber("0499 000 000", "AU") -- TRUE
    ,is_valid_phonenumber("0499 000 000", "US") -- FALSE
    ,format_phonenumber("0499 000 000", "AU") -- +61499000000
    ,is_valid_abn("83 914 571 673") -- TRUE
    ,is_valid_abn("83 914 571 672") -- FALSE