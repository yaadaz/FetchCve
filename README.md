# FetchCve

Fetch CVE data from [NVD](https://nvd.nist.gov/) and save it to the disk.
The output is saved in JSON format, several entries in a file.

## Usage

```
main.py [-h] --days-back DAYS --output-directory OUTPUT
```
Configuration can be changed in the config file.

**Example input:**
```
python main.py --days-back 180 --output_directory ./output
```

## Requirements
* Python 3.11
* requests 2.31
