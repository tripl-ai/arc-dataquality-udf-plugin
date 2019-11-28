arc-dataquality-udf-plugin defines a set of data quality/validation user defined functions.

The intention of this plugin is to provide a standard suite of data quality functions that can be used by many people and companies. Feel free to raise pull requests against this repository with reusable functions that may be beneficial to others.

## User Defined Functions

- `is_valid_phonenumber(numberToParse, defaultRegion)` returns a boolean of whether the phone number appears valid for that region e.g. `is_valid_phonenumber('61499000000', 'AU')`.
- `format_phonenumber(numberToParse, defaultRegion)` returns an [ISO E164](https://en.wikipedia.org/wiki/E.164) formatted phone number string e.g. `format_phonenumber('61499000000', 'AU')` becomes `+61499000000`.
- `is_valid_abn(abn)` returns a boolean of whether the Australian Business Number (ABN) passes the inbuilt checksum function.
- `is_valid_acn(acn)` returns a boolean of whether the Australian Company Number (ACN) passes the inbuilt checksum function.

## License

Arc is released under the [MIT License](https://opensource.org/licenses/MIT).

`is_valid_phonenumber` and `format_phonenumber` use https://github.com/google/libphonenumber released under the [Apache 2.0](https://opensource.org/licenses/Apache-2.0) license.