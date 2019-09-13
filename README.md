arc-datavalidation-udf-plugin defines a set of data validation user defined functions.

## UDFs

- is_valid_phonenumber(numberToParse, defaultRegion) returns a boolean of whether the phone number appears valid for that region.
- format_phonenumber(numberToParse, defaultRegion) returns an [ISO E164](https://en.wikipedia.org/wiki/E.164) formatted phone number string.
- is_valid_abn(abn) returns a boolean of whether the ABN passes the inbuilt checksum function.

## License

Arc is released under the [MIT License](https://opensource.org/licenses/MIT).

`is_valid_phonenumber` and `format_phonenumber` use https://github.com/google/libphonenumber