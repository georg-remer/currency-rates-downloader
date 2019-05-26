# Currency Rates Downloader

This module downloads currency exchange rates, making requests to
API of Central Bank of Russia and National Bank of Ukraine.

# How To

1. Create necessary database objects by runnig commands from 'backup.txt';
2. Rename 'settings_sample.py' to 'settings.py';
3. Specify database and mail server settings in 'settings.py';
4. Run script 'currency_rates_downloader.py'.

You can specify exchange rates for which currencies are needed to be
downloaded by setting True/False in 'download_from_cbr' and 'download_from_nbu'
fields of the 'currency.t_ref_currency' table.

Nota Bene: Central Bank of Russia uses 'currency_identificator_cbr' field, 
whilst National Bank of Ukraine - 'currency_num_code' (Numeric ISO code).

# Release Notes

25.05.2019 Version 1.0.1
- Fixed empty email body bug

25.05.2019 Version 1.0.0
- Released first version
