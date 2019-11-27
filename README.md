arc-dataquality-udf-plugin defines a set of data quality/validation user defined functions.

## User Defined Functions

- `is_valid_phonenumber(numberToParse, defaultRegion)` returns a boolean of whether the phone number appears valid for that region e.g. `is_valid_phonenumber('61499000000', 'AU')`.
- `format_phonenumber(numberToParse, defaultRegion)` returns an [ISO E164](https://en.wikipedia.org/wiki/E.164) formatted phone number string e.g. `format_phonenumber('61499000000', 'AU')` becomes `+61499000000`.
- `is_valid_abn(abn)` returns a boolean of whether the ABN passes the inbuilt checksum function.

## Contributbutions


## License

Arc is released under the [MIT License](https://opensource.org/licenses/MIT).

`is_valid_phonenumber` and `format_phonenumber` use https://github.com/google/libphonenumber released under the [Apache 2.0](https://opensource.org/licenses/Apache-2.0) license.