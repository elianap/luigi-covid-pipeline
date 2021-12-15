# Luigi Covid Pipeline

Managing a ML pipeline with Luigi. 
Case study: reports of the Italian COVID-19 situation. 
We use the data of the Italian Civil Protection, available at this repository.


## Installation

Clone the respository:
```shell
git clone https://github.com/elianap/luigi-covid-pipeline.git
```

You can firstly create an environment, for example using conda:
```shell
conda create --name luigi3 python=3
```

Then install the required package (in requirements.txt) via pip.

To install luigi:
```shell
pip install luigi
```

## Run Luigi

Run luigi scheduler

```shell
PYTHONPATH='.' luigid
```

```shell
luigi --module <module_name> <task_name> --<parameter1_name> <par_value> 
```



## Example

Generate a daily report:

```shell
luigi --module covid_pipeline AggregateInReport
```